from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "key",
        F.when(F.col("host").rlike("^(h3|h2)$"), "GROUP_A").when(F.col("host").rlike("^(h1|h4)$"), "OTHER_ZONE").otherwise("CORE").alias("host_zone"),
    )
    .orderBy(F.col("service").desc_nulls_first())
)
