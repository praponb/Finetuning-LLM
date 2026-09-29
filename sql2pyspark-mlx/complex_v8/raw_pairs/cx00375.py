from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_start_year_session_n = Window.partitionBy(F.year("session_start"), "session_no")
w_all_by_session_end_moving_1 = Window.orderBy(F.col("session_end").desc(), "warehouse_id", "session_no").rowsBetween(-1, Window.currentRow)

base = (
    df_stock_moves
    .filter(F.col("qty") <= 288)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

with_prev = base.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_warehouse_id_running))

per_session = (
    sessionized
    .groupBy("warehouse_id", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("qty").alias("session_qty"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

with_windows = (
    per_session
    .select(
        "warehouse_id",
        "session_no",
        "session_start",
        "session_end",
        "duration_min",
        F.round(F.avg("duration_min").over(w_session_start_year_session_n), 2).alias("session_start_year_session_no_avg_duration_min"),
        F.round(F.col("session_no") / F.nullif(F.sum("session_no").over(w_session_start_year_session_n), F.lit(0)), 4).alias("session_no_share_of_session_start_year_session_n"),
        F.round(F.avg("duration_min").over(w_all_by_session_end_moving_1), 2).alias("duration_min_avg_2"),
    )
)

result = (
    with_windows
    .select(
        "warehouse_id",
        "session_start",
        "session_end",
        "duration_min",
        "session_start_year_session_no_avg_duration_min",
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
    )
    .orderBy(F.col("session_start_year_session_no_avg_duration_min").desc())
)
