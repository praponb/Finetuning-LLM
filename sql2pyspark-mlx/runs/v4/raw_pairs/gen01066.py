from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .select("id", "host", "log_ts", "level", "k", "attr_value")
    .orderBy(F.col("log_ts").desc_nulls_first(), "k")
)
