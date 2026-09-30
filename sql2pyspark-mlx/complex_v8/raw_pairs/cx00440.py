from pyspark.sql import functions as F

filtered = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.when(F.col("premium") >= 3532.7, "HIGH").when(F.col("premium") >= 2319.8, "MEDIUM").otherwise("LOW").alias("premium_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status").isNotNull()) & (F.col("p.start_date").between(F.lit("2024-03-15").cast("date"), F.lit("2025-07-07").cast("date"))), "left_semi")
)

qualified = (
    filtered_2.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status").isin("APPROVED", "IN_REVIEW", "OPEN")), "left_semi")
)

exploded_detail_claims = (
    qualified.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product_or_unknown",
        "s.status_slug",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_date",
    )
)

combined_details = (
    exploded_detail_claims.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product_or_unknown",
        "s.status_slug",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_reported_at",
        F.col("p.region").alias("holder_region"),
        "c.claim_type",
        F.col("c.reported_at").alias("claim_reported_at_2"),
    )
)

enriched_holder = (
    combined_details.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product_or_unknown",
        "s.status_slug",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_reported_at",
        "s.holder_region",
        "s.claim_reported_at_2",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

scored = (
    enriched_holder
    .filter(F.col("claim_reported_at") < F.lit("2025-03-08 12:00:00").cast("timestamp"))
    .select(
        "id",
        "claim_id",
        "claim_reported_at",
        "holder_region",
        "claim_reported_at_2",
        "holder_joined_on",
        "holder_risk_score",
        "holder_segment",
        F.length(F.trim(F.col("product_or_unknown"))).alias("product_or_unknown_len"),
    )
)

result = (
    scored
    .select(
        "id",
        "claim_id",
        "holder_region",
        "claim_reported_at_2",
        "holder_joined_on",
        "holder_segment",
        "product_or_unknown_len",
        F.when(F.col("holder_segment").isin("VIP", "RETAIL"), "group_a").when(F.col("holder_segment") == "SME", "group_b").otherwise("other").alias("holder_segment_group"),
        F.when(F.col("holder_risk_score") >= 0.6, "large").when(F.col("holder_risk_score") >= 0.3, "medium").otherwise("small").alias("holder_risk_score_tier"),
    )
    .orderBy("product_or_unknown_len", F.col("claim_reported_at_2").desc())
)
