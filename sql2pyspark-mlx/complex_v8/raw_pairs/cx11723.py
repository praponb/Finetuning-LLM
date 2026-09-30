from pyspark.sql import functions as F

base = (
    df_policies
    .filter((F.col("premium") <= 3465.4) | (F.col("start_date") < F.lit("2025-02-13").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat_ws("|", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.count("*").alias("policy_count"))
)

profiled = (
    base.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

result = (
    profiled
    .groupBy("holder_id", "policy_count")
    .pivot("product", ["HOME", "LIFE"])
    .agg(F.sum("premium"))
    .orderBy("holder_id", "policy_count")
)
