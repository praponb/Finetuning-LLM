from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

tagged = (
    cleaned_claims
    .filter(~F.col("claim_type").isin("theft", "flood"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "days_since_claim_date",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.round(F.col("approved_amount") * 0.9, 2).alias("approved_amount_adj"),
    )
)

joined = (
    tagged.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") >= 2), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.days_since_claim_date",
        "s.claim_type_len",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

enriched = (
    joined.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.days_since_claim_date",
        "s.claim_type_len",
        "s.adjuster_level",
        "s.adjuster_name",
        F.col("a.level").alias("adjuster_level_2"),
        F.col("p.product").alias("policy_product"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.upper(F.trim(F.col("a.adjuster_name"))).like("G%")), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "days_since_claim_date",
        "adjuster_level",
        "adjuster_level_2",
        "policy_product",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
        (F.col("claim_type_len") - F.col("adjuster_level_2")).alias("claim_type_len_minus_adjuster_level_2"),
    )
    .orderBy(F.col("claim_type_slug").asc_nulls_last())
)
