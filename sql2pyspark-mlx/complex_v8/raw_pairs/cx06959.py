from pyspark.sql import functions as F

joined_ranges = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 60)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_60d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_60d"),
    )
)

with_parent = (
    joined_ranges.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_60d",
        "s.prior_approved_amount_60d",
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "policy_id",
        "claim_date",
        "policy_product",
        F.coalesce(F.col("prior_rows_60d"), F.lit(0)).alias("prior_rows_60d_filled"),
        F.concat(F.col("policy_product"), F.lit("#"), F.col("id").cast("string")).alias("policy_product_ref"),
    )
    .orderBy(F.col("claim_date").desc(), F.col("policy_product").desc_nulls_first())
)
