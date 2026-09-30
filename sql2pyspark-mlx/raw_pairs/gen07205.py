from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .groupingSets([["service", "host"], ["service"], []], "service", "host")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("log_ts").alias("first_log_ts"),
        F.max("log_ts").alias("ended_at"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids") > 1)
)
