from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-08-27").cast("date")) & (F.col("end_date") < F.lit("2026-09-19").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
        F.round(F.col("premium") * 0.25, 2).alias("premium_adj"),
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
    )
)

detail_rows = (
    base_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.start_date_quarter",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

with_details_claim = (
    detail_rows.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.start_date_quarter",
        "s.claim_id",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.claimed_amount").alias("claim_claimed_amount_2"),
    )
)

result = (
    with_details_claim
    .groupingSets(
        [["claim_adjuster_id", "claim_claim_type"], ["claim_adjuster_id"]],
        "claim_adjuster_id", "claim_claim_type",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("claim_claimed_amount_2"), 2).alias("stddev_claim_claimed_amount_2"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("claim_adjuster_id").asc_nulls_last(),
        F.col("claim_claim_type").asc_nulls_last(),
    )
)
