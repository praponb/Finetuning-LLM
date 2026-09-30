from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

scored = (
    flattened
    .withColumn("status_code_rank", F.rank().over(w))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w))
)

valid = (
    scored
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .withColumn("status_code_rank_per_status_code_cume_dist", (F.col("status_code_rank") / F.nullif("status_code_cume_dist", F.lit(0))))
    .withColumn("k_filled", F.coalesce("k", F.lit("OTHER")))
)

valid_app_logs = (
    valid
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull() & (F.col("host") == "h3"))
    .withColumn("least_latency_ms_status_code_cume_dist", F.least("latency_ms", "status_code_cume_dist"))
    .withColumn("resolved_k", F.coalesce("k", F.lit("NONE")))
)

result = (
    valid_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "v",
        "status_code_rank",
        "status_code_cume_dist",
        "status_code_rank_per_status_code_cume_dist",
        "k_filled",
        "least_latency_ms_status_code_cume_dist",
        F.when(F.col("host").rlike("^(h3|h4)$"), "GROUP_A").when(F.col("host").rlike("^(h1|h2)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("host_zone"),
    )
    .orderBy(F.col("host_zone").desc())
)
