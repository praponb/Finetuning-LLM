from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("debug_rows").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .groupBy(
        "attr_key",
        F.when(F.col("latency_ms") >= 2127, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
        "level",
    )
    .agg(
        F.sum(F.when(F.col("level") == "INFO", F.col("status_code")).otherwise(0)).alias("info_status_code"),
        F.percentile_approx("status_code", 0.9).alias("status_code_p90"),
        F.min("log_ts").alias("started_at"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
    )
    .withColumn("debug_rows_dense_rank", F.dense_rank().over(w))
)
