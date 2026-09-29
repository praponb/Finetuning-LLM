from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "host",
        "log_ts",
        "level",
        "key",
        "attr_value",
        F.when(F.col("host").rlike("^(h3|h1)$"), "GROUP_B").when(F.col("host").rlike("^(h2|h4)$"), "EXTENDED").otherwise("GROUP_A").alias("host_zone"),
    )
)
