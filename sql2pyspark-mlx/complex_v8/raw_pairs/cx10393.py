from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_moves = (
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

with_prev = prepared_moves.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_operator_running))

session_summary = (
    sessionized
    .groupBy("operator", "session_id")
    .agg(
        F.min("moved_at").alias("session_start"),
        F.max("moved_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("moved_at")) - F.unix_timestamp(F.min("moved_at"))) / 60, 1).alias("duration_min"),
    )
)

tagged = session_summary.withColumn("operator_prefix", F.substring(F.trim(F.col("operator")), 1, 3))

enriched = (
    tagged
    .select(
        "operator",
        "session_id",
        "session_end",
        "duration_min",
        "operator_prefix",
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("session_start")).alias("days_since_session_start"),
    )
)

result = (
    enriched
    .select(
        "operator",
        "session_id",
        "duration_min",
        "duration_min_bucket",
        "days_since_session_start",
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
    .orderBy(F.col("session_id").desc(), F.col("days_since_session_start").desc())
)
