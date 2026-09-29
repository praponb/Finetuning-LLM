from pyspark.sql import functions as F

covered = (
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

annotated = (
    covered
    .filter(F.col("prior_rows_60d") < 4)
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.when(F.col("prior_rows_60d") >= 5, "A").when(F.col("prior_rows_60d") >= 3, "B").otherwise("C").alias("prior_rows_60d_tier"),
        F.abs(F.col("prior_rows_60d") - 4).alias("prior_rows_60d_distance"),
    )
)

tagged = (
    annotated
    .filter(F.col("prior_rows_60d_distance") <= 5)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_60d",
        "prior_approved_amount_60d",
        "days_since_claim_date",
        "prior_rows_60d_distance",
        (F.floor(F.col("days_since_claim_date") / 100) * 100).alias("days_since_claim_date_bucket"),
    )
)

with_details_policy = (
    tagged.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select("s.*", F.col("p.product").alias("policy_product"), "po.holder_name")
)

result = (
    with_details_policy
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_60d",
        "prior_approved_amount_60d",
        "days_since_claim_date",
        "prior_rows_60d_distance",
        "days_since_claim_date_bucket",
        "holder_name",
    )
    .orderBy(F.col("prior_rows_60d_distance").desc())
)
