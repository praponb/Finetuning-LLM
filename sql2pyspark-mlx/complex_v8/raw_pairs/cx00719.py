from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_session_end_year = Window.partitionBy("event_count", F.year("session_end")).orderBy("session_qty", "warehouse_id", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_session_end_year_2 = Window.partitionBy("event_count", F.year("session_end")).orderBy("session_qty")
w_warehouse_id_by_duration_min = Window.partitionBy("warehouse_id").orderBy("duration_min", "warehouse_id", "burst_id")

base = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

with_prev = base.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_warehouse_id_running))

sessions = (
    sessionized
    .groupBy("warehouse_id", "burst_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    sessions
    .select(
        "warehouse_id",
        "burst_id",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        F.sum("duration_min").over(w_event_count_session_end_year).alias("running_duration_min"),
        F.rank().over(w_event_count_session_end_year_2).alias("session_qty_rank"),
        F.round(F.percent_rank().over(w_warehouse_id_by_duration_min), 3).alias("duration_min_percent_rank"),
    )
)

result = (
    metrics
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "warehouse_id",
        "burst_id",
        "session_start",
        "duration_min",
        "running_duration_min",
        "session_qty_rank",
        "duration_min_percent_rank",
        F.greatest(F.col("burst_id"), F.col("duration_min")).alias("greatest_burst_id_duration_min"),
        (F.col("event_count") - F.col("session_qty_rank")).alias("event_count_minus_session_qty_rank"),
    )
    .orderBy(F.col("duration_min").desc(), "warehouse_id", "burst_id")
    .limit(10)
)
