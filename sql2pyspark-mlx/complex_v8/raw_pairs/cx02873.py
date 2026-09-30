from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter(F.col("premium") > 1051.9)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.max("premium").alias("max_policy_premium"))
)

with_stats = (
    valid_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.max_policy_premium")
)

result = (
    with_stats
    .groupBy("holder_id", "start_date")
    .pivot("status", ["EXPIRED", "CANCELLED"])
    .agg(F.sum("premium").alias("amount"), F.max("premium").alias("max_v"))
)
