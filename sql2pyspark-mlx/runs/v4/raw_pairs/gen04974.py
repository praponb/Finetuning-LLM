from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .groupBy("host")
    .agg(
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.max("status_code").alias("max_status_code"),
    )
    .filter(F.col("max_status_code") < 0)
    .orderBy(F.col("host").desc())
)
