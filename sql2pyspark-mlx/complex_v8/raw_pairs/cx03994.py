from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.year(F.col("claim_date")) == 2024) & (F.col("claim_type") == "medical"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        F.round(F.col("claimed_amount") * 1.5, 2).alias("claimed_amount_adj"),
    )
)

combined_details_adjuster = (
    filtered_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level").between(2, 3)), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.claimed_amount_adj",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.product").alias("policy_product"),
    )
)

enriched = (
    combined_details_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "claimed_amount_adj",
        "adjuster_level",
        "policy_product",
        (F.col("adjuster_level") + F.col("approved_amount")).alias("adjuster_level_plus_approved_amount"),
        F.lower(F.regexp_replace(F.trim(F.col("policy_product")), " ", "_")).alias("policy_product_slug"),
        F.when(F.col("adjuster_level") >= 3, "A").when(F.col("adjuster_level") >= 2, "B").otherwise("C").alias("adjuster_level_tier"),
    )
)

detail_rows = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.adjuster_level",
        "s.policy_product",
        "s.adjuster_level_plus_approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

with_parent = (
    detail_rows.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .filter(F.col("a.team") == "Motor")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.adjuster_level",
        "s.policy_product",
        "s.adjuster_level_plus_approved_amount",
        "s.claim_id",
        "s.claim_approved_amount",
        "s.claim_reported_at",
        F.col("a.team").alias("adjuster_team"),
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
        F.col("c.reported_at").alias("claim_reported_at_2"),
    )
)

combined_details = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (F.col("c.claimed_amount").between(6745.1, 42447.2)), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.policy_product",
        "s.adjuster_level_plus_approved_amount",
        "s.claim_id",
        "s.claim_approved_amount_2",
        F.col("c.approved_amount").alias("claim_approved_amount_3"),
        "c.claim_date",
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["policy_product", "policy_id"], ["policy_product"]],
        "policy_product", "policy_id",
    )
    .agg(
        F.grouping("policy_product").alias("is_total_policy_product"),
        F.array_sort(F.collect_set("policy_status")).alias("policy_status_list"),
        F.count("*").alias("row_count"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
    )
    .select(
        F.coalesce(F.col("policy_product"), F.lit("ALL")).alias("policy_product"),
        "policy_id",
        "is_total_policy_product",
        "policy_status_list",
        "row_count",
        "claim_date_span_days",
    )
    .orderBy(
        F.col("policy_product").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
