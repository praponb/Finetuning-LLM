from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_by_n_events_full = Window.partitionBy("burst_id").orderBy(F.col("n_events").desc_nulls_first(), "sku_id", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_events = Window.partitionBy("n_events")

base_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

with_prev = base_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_sku_id_running))

sessions = (
    sessionized
    .groupBy("sku_id", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    sessions
    .select(
        "*",
        F.first("session_start").over(w_burst_id_by_n_events_full).alias("first_session_start"),
        F.round(F.col("burst_id") - F.avg("burst_id").over(w_n_events), 2).alias("burst_id_vs_n_events_avg"),
    )
)

result = (
    metrics
    .select(
        "sku_id",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        "session_unit_price",
        "duration_min",
        "first_session_start",
        "burst_id_vs_n_events_avg",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("session_end")).alias("days_since_session_end"),
    )
    .orderBy(F.col("days_since_session_end").desc(), F.col("sku_id").desc())
)
