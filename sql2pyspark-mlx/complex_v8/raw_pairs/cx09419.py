from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter(F.col("product") == "LIFE")
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.last_day(F.col("end_date")).alias("end_date_month_end"),
        F.when(F.col("premium") >= 3465.4, "A").when(F.col("premium") >= 2033.8, "B").otherwise("C").alias("premium_tier"),
    )
)

tagged = (
    valid_policys
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.concat_ws("-", F.col("premium_tier"), F.col("status")).alias("premium_tier_status_label"),
    )
)

enriched_holder = (
    tagged.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.upper(F.trim(F.col("p.holder_name"))).like("D%")), "inner")
    .filter(F.col("p.region").isin("East", "South"))
    .select(
        "s.*",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status") == "APPROVED", 1)).alias("claim_status_count"),
        F.count("*").alias("claim_count"),
    )
)

with_counts = (
    enriched_holder.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "premium_tier",
        "status_slug",
        "premium_tier_status_label",
        "holder_region",
        "holder_risk_score",
        "holder_joined_on",
        "claim_status_count",
        "claim_count",
    )
    .orderBy("id")
)
