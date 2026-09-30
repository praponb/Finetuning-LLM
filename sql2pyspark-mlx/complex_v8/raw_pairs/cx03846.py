from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

with_prev = filtered.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_sku_id_running))

sessions = (
    sessionized
    .groupBy("sku_id", "session_no")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

scored = (
    sessions
    .select(
        "*",
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
        F.to_date(F.col("session_end")).alias("session_end_date"),
    )
)

result = (
    scored
    .select(
        "sku_id",
        "event_count",
        "duration_min",
        "duration_min_filled",
        "session_end_date",
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("session_end")).alias("days_since_session_end"),
    )
    .orderBy(F.col("duration_min_bucket").desc(), F.col("session_end_date").desc())
)
