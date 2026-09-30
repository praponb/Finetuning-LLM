from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter((F.col("product") == "LIFE") & (F.col("end_date").between(F.lit("2025-11-20").cast("date"), F.lit("2026-10-23").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
    )
)

tagged = (
    scoped_policys
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
    )
)

joined_holder = (
    tagged.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment").isin("SME", "RETAIL")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.product_clean",
        "s.product_prefix",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

enriched_holder = (
    joined_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.product_clean",
        "s.product_prefix",
        "s.holder_region",
        "s.holder_joined_on",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
    )
)

filtered = (
    enriched_holder.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") > 2289.0) & (F.col("p.end_date").between(F.lit("2025-12-05").cast("date"), F.lit("2027-05-16").cast("date"))), "left_anti")
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
        F.count("*").alias("policy_count"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.avg_policy_premium",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "product",
        "start_date",
        "product_prefix",
        "holder_region",
        "holder_joined_on",
        "holder_name",
        "holder_segment",
        "policy_count",
        F.least(F.col("avg_policy_premium"), F.col("policy_count")).alias("least_avg_policy_premium_policy_count"),
        F.coalesce(F.col("policy_count"), F.lit(0)).alias("policy_count_filled"),
    )
    .orderBy(F.col("holder_segment").desc_nulls_first(), "id")
    .limit(10)
)
