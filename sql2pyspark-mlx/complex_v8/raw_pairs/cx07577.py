from pyspark.sql import functions as F

prepared = (
    df_policyholders
    .filter(F.col("risk_score") > 0.1)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region_or_unknown"),
    )
)

scored = prepared.select("*", F.date_add(F.col("joined_on"), 30).alias("joined_on_plus_30d"))

line_level = (
    scored.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.end_date").isNotNull()), "left")
    .select(
        "s.id",
        "s.segment",
        "s.region_or_unknown",
        F.col("p.id").alias("policy_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        F.col("p.status").alias("policy_status"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

filtered = (
    line_level.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.product").isNotNull()), "left_semi")
)

with_activity = (
    filtered.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.status") == "LAPSED") & (F.col("p.product").isin("HOME", "LIFE")), "left_semi")
)

exploded_detail_policys = (
    with_activity.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.segment",
        "s.region_or_unknown",
        "s.policy_id",
        "s.policy_end_date",
        "s.policy_status",
        "s.policy_start_date",
        F.col("p.id").alias("policy_id_2"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date_2"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

summarized_rows = (
    exploded_detail_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "policy_end_date",
        "policy_status",
        "policy_start_date",
        "policy_id_2",
        "policy_premium",
        F.round(F.col("policy_premium") * 1.5, 2).alias("policy_premium_adj"),
    )
)
