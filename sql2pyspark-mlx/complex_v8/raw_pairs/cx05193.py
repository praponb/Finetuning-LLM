from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2025-10-29").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

qualified = (
    base_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_type") == "flood"), "left_semi")
)

enriched = (
    qualified.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.product").isNotNull()), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("po.region").alias("holder_region"),
        F.col("po.joined_on").alias("holder_joined_on"),
    )
)

tagged = (
    enriched
    .select(
        "*",
        F.when(F.col("holder_region").isin("North", "Central"), "group_a").when(F.col("holder_region") == "East", "group_b").otherwise("other").alias("holder_region_group"),
        F.quarter(F.col("holder_joined_on")).alias("holder_joined_on_quarter"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

detail_rows = (
    tagged.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.reported_at",
        "s.holder_region_group",
        "s.holder_joined_on_quarter",
        "s.status_slug",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

enriched_2 = (
    detail_rows.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.reported_at",
        "s.holder_region_group",
        "s.holder_joined_on_quarter",
        "s.status_slug",
        "s.claim_id",
        "s.claim_policy_id",
        "s.claim_claimed_amount",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

result = (
    enriched_2
    .filter(F.col("claimed_amount").between(10673.3, 32878.5))
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "reported_at",
        "holder_region_group",
        "holder_joined_on_quarter",
        "status_slug",
        "claim_id",
        "claim_policy_id",
        "claim_claimed_amount",
        "adjuster_level",
        "adjuster_team",
        "adjuster_name",
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
    .orderBy(F.col("adjuster_name").desc(), "id", "claim_id")
    .limit(20)
)
