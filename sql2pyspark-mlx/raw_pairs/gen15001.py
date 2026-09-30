from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "key",
        "value",
        F.when(F.col("host").rlike("^(h1|h2)$"), "PRIORITY").otherwise("EXTENDED").alias("host_zone"),
    )
)
