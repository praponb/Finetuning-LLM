from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .cube("service")
    .agg(
        F.count_if(F.col("host") == "h4").alias("h4_events"),
        F.count_if(F.col("service") == "billing").alias("billing_events"),
    )
    .filter(F.col("billing_events") > 1)
    .orderBy(F.col("service").desc_nulls_first())
)
