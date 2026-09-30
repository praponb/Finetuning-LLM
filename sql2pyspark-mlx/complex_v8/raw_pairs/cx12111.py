from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-10-22").cast("date")) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
    )
)

detail_rows_claims = (
    valid_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.product_ref",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_date",
        F.col("c.status").alias("claim_status"),
    )
)

exploded_detail_claims = (
    detail_rows_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") >= 13635.1), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.product_ref",
        "s.claim_id",
        "s.claim_reported_at",
        "s.claim_status",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at_2"),
        "c.claim_type",
    )
)

result = (
    exploded_detail_claims
    .groupingSets(
        [["claim_id", "claim_type"], ["claim_id"], []],
        "claim_id", "claim_type",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.percentile_approx("claim_claimed_amount", 0.5).alias("median_claim_claimed_amount"),
        F.countDistinct("claim_id").alias("distinct_claim_ids"),
    )
    .select(
        "claim_id",
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "grouping_level",
        "median_claim_claimed_amount",
        "distinct_claim_ids",
    )
    .orderBy(
        F.col("claim_id").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
