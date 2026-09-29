from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_running = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_events_move_type = Window.partitionBy("n_events", "move_type")
w_n_events_by_session_end_runn = Window.partitionBy("n_events").orderBy("session_end", "move_type", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_events_by_session_end = Window.partitionBy("n_events").orderBy("session_end", "move_type", "session_id")

recent = (
    df_stock_moves
    .filter(F.col("sku_id") >= 12)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
    )
)

with_prev = recent.withColumn("prev_moved_at", F.lag("moved_at").over(w_move_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_move_type_running))

session_summary = (
    sessionized
    .groupBy("move_type", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    session_summary
    .select(
        "*",
        F.round(F.col("n_events") - F.avg("n_events").over(w_n_events_move_type), 2).alias("n_events_vs_n_events_move_type_avg"),
        F.round(F.col("session_id") - F.avg("session_id").over(w_n_events_move_type), 2).alias("session_id_vs_n_events_move_type_avg"),
        F.first("session_start", ignorenulls=True).over(w_n_events_by_session_end_runn).alias("first_known_session_start"),
        F.ntile(10).over(w_n_events_by_session_end).alias("session_end_decile"),
    )
)

with_activity = (
    metrics
    .crossJoin(metrics.agg(F.min("session_end_decile").alias("overall_min_session_end_decile")))
    .filter(F.col("session_end_decile") > F.col("overall_min_session_end_decile"))
    .drop("overall_min_session_end_decile")
)

result = (
    with_activity
    .select(
        "move_type",
        "session_id",
        "session_start",
        "n_events",
        "session_unit_price",
        "duration_min",
        "n_events_vs_n_events_move_type_avg",
        "first_known_session_start",
        "session_end_decile",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
    .orderBy("n_events_vs_n_events_move_type_avg")
)
