from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_n_events = Window.partitionBy("sku_id").orderBy("n_events", "sku_id", "burst_id")
w_n_events = Window.partitionBy("n_events")
w_sku_id_by_n_events_moving_3 = Window.partitionBy("sku_id").orderBy("n_events", "sku_id", "burst_id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_sku_id_running))

sessions = (
    sessionized
    .groupBy("sku_id", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    sessions
    .select(
        "*",
        F.round((F.unix_timestamp("session_end") - F.unix_timestamp(F.lag("session_end").over(w_sku_id_by_n_events))) / 60, 1).alias("minutes_since_prev_session_end"),
        F.count(F.lit(1)).over(w_n_events).alias("rows_in_n_events"),
        F.round(F.avg("burst_id").over(w_sku_id_by_n_events_moving_3), 2).alias("burst_id_avg_4"),
        F.ntile(3).over(w_sku_id_by_n_events).alias("n_events_tercile"),
        F.max("duration_min").over(w_n_events).alias("n_events_max_duration_min"),
    )
)

result = (
    ranked
    .select(
        "sku_id",
        "session_start",
        "session_end",
        "duration_min",
        "minutes_since_prev_session_end",
        "rows_in_n_events",
        "burst_id_avg_4",
        "n_events_tercile",
        "n_events_max_duration_min",
    )
    .orderBy(F.col("session_start").desc())
)
