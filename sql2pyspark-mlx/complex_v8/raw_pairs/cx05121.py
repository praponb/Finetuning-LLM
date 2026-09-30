from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter(~F.col("product").isin("HOME", "AUTO"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
    )
)

joined_holder = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.segment").isNotNull())
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        F.col("p.segment").alias("holder_segment"),
    )
)

per_group = (
    joined_holder
    .filter(F.col("holder_id").isNotNull())
    .groupBy(
        F.date_trunc("month", "end_date").alias("end_date_month"),
        "holder_segment",
        "holder_id",
    )
    .agg(
        F.count(F.when(F.col("holder_segment").isin("VIP", "RETAIL"), 1)).alias("n_holder_segment_in_vip_retail"),
        F.count("*").alias("row_count"),
        F.max("end_date").alias("last_end_date"),
    )
)

detail_rows = (
    per_group.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "inner")
    .select(
        "s.end_date_month",
        "s.holder_segment",
        "s.holder_id",
        F.col("p.id").alias("policy_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.count("*").alias("policy_count"))
)

summarized_rows = (
    detail_rows.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

result = (
    summarized_rows
    .rollup("policy_id", "holder_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.percentile_approx("policy_count", 0.5).alias("median_policy_count"),
    )
)
