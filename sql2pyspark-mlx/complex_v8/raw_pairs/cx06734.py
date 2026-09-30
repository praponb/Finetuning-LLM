from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_warehouse_id_running))

sessions = (
    sessionized
    .groupBy("warehouse_id", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    sessions
    .filter(F.col("n_events").between(2, 4))
    .select(
        "warehouse_id",
        "burst_id",
        "session_start",
        "session_end",
        "session_qty",
        "duration_min",
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
    )
)

enriched = (
    labeled
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "*",
        (F.col("session_start") + F.expr("INTERVAL 2 HOURS")).alias("session_start_plus_2h"),
        F.date_format(F.col("session_start"), "MMM yyyy").alias("session_start_label"),
    )
)

result = (
    enriched
    .filter(F.col("burst_id") > 2)
    .select(
        "warehouse_id",
        "burst_id",
        "session_end",
        "session_qty",
        "duration_min",
        "duration_min_filled",
        "session_start_plus_2h",
    )
    .orderBy(F.col("warehouse_id").asc_nulls_last())
)
