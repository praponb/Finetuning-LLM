from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("hour_window").orderBy(F.col("distinct_traces").desc())

parsed = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context")["trace"].alias("trace"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
    )
)

result = (
    parsed
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("retry_count") >= 4, "PREMIUM").otherwise("STANDARD").alias("retry_count_tier"),
    )
    .agg(
        F.countDistinct("trace").alias("distinct_traces"),
        F.percentile_approx("retry_count", [0.25, 0.5, 0.75]).alias("retry_count_quartiles"),
        F.max("log_ts").alias("last_seen"),
    )
    .withColumn("distinct_traces_rank", F.rank().over(w))
    .orderBy(F.col("retry_count_tier").desc())
)
