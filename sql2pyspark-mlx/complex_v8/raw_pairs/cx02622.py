from pyspark.sql import functions as F

base = (
    df_policies
    .filter((F.col("product") == "AUTO") & (F.col("premium") >= 2033.8))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

grouped = (
    base
    .groupBy(
        F.date_trunc("month", "end_date").alias("end_date_month"),
        "holder_id",
        "product",
    )
    .agg(
        F.max("premium").alias("max_premium"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("product").isin("HOME", "HEALTH"), 1)).alias("n_product_in_home_health"),
        F.count("*").alias("n_rows"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.max("status_len").alias("max_status_len"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.min("premium").alias("min_policy_premium"),
    )
)

with_counts = (
    grouped.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        "p.min_policy_premium",
    )
)

enriched_holder = (
    with_counts.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.end_date_month",
        "s.holder_id",
        "s.product",
        "s.status_list",
        "s.n_rows",
        "s.distinct_holder_ids",
        "s.policy_count",
        "s.min_policy_premium",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

screened = (
    enriched_holder.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (~F.col("p.product").isin("HEALTH", "HOME")) & (F.col("p.premium") >= 1541.8), "left_anti")
)

with_parent_holder = (
    screened.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region").isin("East", "West")), "inner")
    .filter(F.col("p.segment") != "SME")
    .select(
        "s.end_date_month",
        "s.holder_id",
        "s.product",
        "s.status_list",
        "s.n_rows",
        "s.distinct_holder_ids",
        "s.policy_count",
        "s.min_policy_premium",
        "s.holder_joined_on",
        "s.holder_segment",
        F.col("p.segment").alias("holder_segment_2"),
    )
)

result = (
    with_parent_holder
    .select(
        "end_date_month",
        "holder_id",
        "status_list",
        "n_rows",
        "policy_count",
        "holder_joined_on",
        "holder_segment",
        "holder_segment_2",
        F.year(F.col("holder_joined_on")).alias("holder_joined_on_year"),
    )
    .orderBy("holder_joined_on")
)
