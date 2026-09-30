from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("premium") >= 3544.1, "HIGH").when(F.col("premium") >= 1541.8, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

rollup_base = (
    filtered_policys
    .groupBy("holder_id", "premium_tier", F.year("end_date").alias("end_date_year"))
    .agg(
        F.round(100 * F.count(F.when(F.col("product") == "TRAVEL", 1)) / F.count("*"), 1).alias("pct_product_travel"),
        F.count(F.when(F.col("product") != "AUTO", 1)).alias("n_product_auto"),
        F.max("premium").alias("max_premium"),
        F.count("*").alias("cnt"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
    )
    .filter(F.col("cnt") >= 2)
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(~F.col("status").isin("LAPSED", "CANCELLED"), 1)).alias("policy_status_count"),
        F.count("*").alias("policy_count"),
    )
)

with_stats = (
    rollup_base.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

joined = (
    with_stats.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on").between(F.lit("2024-02-22").cast("date"), F.lit("2025-09-02").cast("date")))
    .select("s.*", F.col("p.joined_on").alias("holder_joined_on"))
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("start_date").alias("first_policy_start_date"),
        F.max("premium").alias("max_policy_premium"),
    )
)

profiled = (
    joined.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.first_policy_start_date", "p.max_policy_premium")
)

result = (
    profiled
    .select(
        "holder_id",
        "n_product_auto",
        "max_premium",
        "cnt",
        "avg_premium",
        "policy_status_count",
        "policy_count",
        "holder_joined_on",
        "first_policy_start_date",
        "max_policy_premium",
        F.abs(F.col("policy_count") - 2).alias("policy_count_distance"),
    )
    .orderBy(
        F.col("policy_status_count").desc_nulls_first(),
        F.col("holder_id").desc_nulls_first(),
    )
)
