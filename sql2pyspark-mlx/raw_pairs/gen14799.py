from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("status_code")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w3 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("service", "k").orderBy("log_ts", "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

ranked = (
    flattened
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_gaps = (
    ranked
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w2).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w2) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        F.sum("is_new_session").over(w3).alias("session_id"),
    )
)

ranked_app_logs = (
    islands
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

summary = (
    ranked_app_logs
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("status_code") >= 410, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.count(F.when(F.col("k") == "version", 1)).alias("version_rows"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.percentile_approx("session_id", 0.95).alias("session_id_p95"),
    )
)

result = (
    summary
    .select("hour_window", "status_code_tier", "status_code_bit_or", "avg_session_id")
)
