from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_moves = (
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

with_prev = scoped_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_sku_id_running))

sessions = (
    sessionized
    .groupBy("sku_id", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

stats = (
    sessions
    .groupBy(
        "sku_id",
        F.date_trunc("month", "session_end").alias("session_end_month"),
        F.date_trunc("month", "session_start").alias("session_start_month"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.min("duration_min").alias("min_duration_min"),
        F.sum("session_id").alias("total_session_id"),
        F.min("events").alias("min_events"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
    )
    .filter(F.col("n_rows") >= 2)
)

derived = (
    stats
    .filter(F.col("n_rows") > 2)
    .select(
        "*",
        (F.col("session_end_month") + F.expr("INTERVAL 7 DAYS")).alias("session_end_month_plus_7d"),
        F.round(F.col("total_session_id") / F.nullif(F.col("min_events"), F.lit(0)), 3).alias("total_session_id_per_min_events"),
    )
)

result = (
    derived
    .filter(F.col("min_events") < 5)
    .select(
        "sku_id",
        "session_start_month",
        "n_rows",
        "min_duration_min",
        "min_events",
        "distinct_sku_ids",
        "session_end_month_plus_7d",
        "total_session_id_per_min_events",
        F.when(F.col("min_duration_min") >= 132.0, "large").when(F.col("min_duration_min") >= 48.0, "medium").otherwise("small").alias("min_duration_min_tier"),
    )
    .orderBy(F.col("min_duration_min_tier").desc(), F.col("sku_id").desc())
)
