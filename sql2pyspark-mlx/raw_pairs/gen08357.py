from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "service", "host", "level", F.explode("context").alias("k", "v"))
)

result = (
    lines
    .select(
        "id",
        "service",
        "host",
        "k",
        "v",
        F.when(F.col("k").rlike("^(version|region)$"), "CORE").when(F.col("k").rlike("^(trace)$"), "EXTENDED").otherwise("GROUP_B").alias("k_zone"),
    )
    .orderBy(F.col("host").desc_nulls_first())
)
