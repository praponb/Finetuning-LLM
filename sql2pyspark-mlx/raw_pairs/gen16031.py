from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "service", "log_ts", F.explode("context").alias("key", "attr_value"))
)

result = (
    flattened
    .select(
        "id",
        "log_ts",
        "key",
        F.when(F.col("key").rlike("^(region|version)$"), "CORE").otherwise("GROUP_B").alias("key_zone"),
    )
)
