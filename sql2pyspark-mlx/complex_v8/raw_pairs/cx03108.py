from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.col("end_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

with_activity = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment") == "SME") & (F.col("p.joined_on") >= F.lit("2024-07-18").cast("date")), "left_anti")
)

exploded_detail = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product_len",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
    )
)

exploded_detail_2 = (
    exploded_detail.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") >= 0.0), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product_len",
        "s.claim_id",
        "s.claim_reported_at",
        F.col("c.id").alias("claim_id_2"),
        "c.claim_date",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

qualified = (
    exploded_detail_2.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date") >= F.lit("2024-06-19").cast("date")), "left_semi")
)

result = (
    qualified
    .cube("product_len", "claim_id", "claim_id_2")
    .agg(
        F.round(F.sum("claim_claimed_amount"), 2).alias("total_claim_claimed_amount"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("product_len").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
        F.col("claim_id_2").asc_nulls_last(),
    )
)
