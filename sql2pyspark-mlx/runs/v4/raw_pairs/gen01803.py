from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "log_ts", "latency_ms", F.explode("context").alias("k", "v"))
)

result = (
    lines
    .select(
        "id",
        "log_ts",
        "k",
        F.when(F.col("k").rlike("^(version|trace)$"), "EXTENDED").when(F.col("k").rlike("^(region)$"), "GROUP_A").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy("id")
)
