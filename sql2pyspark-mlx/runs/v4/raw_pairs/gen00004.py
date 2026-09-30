from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.orderBy(F.col("status_code").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

enriched = (
    exploded
    .select(
        "*",
        F.avg("status_code").over(w).alias("attr_key_avg_status_code"),
        F.dense_rank().over(w2).alias("status_code_dense_rank"),
    )
)

filtered = (
    enriched
    .filter((F.col("log_ts") >= F.lit("2025-06-01 12:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .withColumn("latency_ms_filled", F.coalesce("latency_ms", F.lit(0)))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

daily = (
    filtered
    .groupBy(
        F.when(F.col("status_code") >= 408, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.corr("latency_ms", "status_code").alias("corr_latency_ms_status_code"),
        F.round(F.avg("attr_key_avg_status_code"), 2).alias("avg_attr_key_avg_status_code"),
    )
)

result = (
    daily
    .select(
        "status_code_tier",
        "corr_latency_ms_status_code",
        "avg_attr_key_avg_status_code",
    )
    .orderBy(F.col("avg_attr_key_avg_status_code").desc())
)
