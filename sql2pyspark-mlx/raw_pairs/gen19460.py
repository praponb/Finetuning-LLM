from pyspark.sql import functions as F

exploded = df_app_logs.select("id", "host", "log_ts", F.explode("context").alias("k", "value"))

result = (
    exploded
    .select(
        "id",
        "host",
        "log_ts",
        "k",
        "value",
        F.when(F.col("host").rlike("^(h1|h2)$"), "GROUP_A").when(F.col("host").rlike("^(h3|h4)$"), "GROUP_B").otherwise("PRIORITY").alias("host_zone"),
    )
    .orderBy(F.col("k").desc_nulls_last())
)
