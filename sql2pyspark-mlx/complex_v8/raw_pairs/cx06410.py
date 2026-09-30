from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at_month", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
    .filter(F.col("move_type") == "OUTBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
    )
)

with_prev = cleaned.withColumn("prev_moved_at_month", F.lag("moved_at_month").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at_month").isNull() | ((F.unix_timestamp("moved_at_month") - F.unix_timestamp("prev_moved_at_month")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_sku_id_running))

per_session = (
    sessionized
    .groupBy("sku_id", "burst_id")
    .agg(
        F.min("moved_at_month").alias("session_start"),
        F.max("moved_at_month").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("unit_price").alias("session_unit_price"),
        F.round((F.unix_timestamp(F.max("moved_at_month")) - F.unix_timestamp(F.min("moved_at_month"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .select(
        "sku_id",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("session_start")).alias("days_since_session_start"),
        F.greatest(F.col("burst_id"), F.col("n_events")).alias("greatest_burst_id_n_events"),
    )
    .orderBy(F.col("session_start").desc())
)
