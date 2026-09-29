from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter(F.col("end_date") < F.lit("2026-03-13").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.when(F.col("premium") >= 4670.9, "A").when(F.col("premium") >= 2033.8, "B").otherwise("C").alias("premium_tier"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

exploded_detail_claims = (
    valid_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") > 0.0), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.premium_tier",
        "s.premium_bucket",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
    )
)

with_counts = (
    exploded_detail_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.claim_adjuster_id"), "left")
    .select("s.*", "c.first_claim_reported_at", "c.avg_claim_claimed_amount")
)

enriched_adjuster = (
    with_counts.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id"), "left")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    enriched_adjuster
    .cube("claim_id", "claim_adjuster_id", "adjuster_team")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(~F.col("adjuster_team").isin("Property", "Travel"), F.col("premium"))).alias("premium_adjuster_team_not_in_propert"),
    )
)
