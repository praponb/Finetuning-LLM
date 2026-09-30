from pyspark.sql import functions as F

in_window = (
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

combined_details_policy = (
    in_window.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.product") == "TRAVEL")
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
    combined_details_policy
    .filter(F.col("policy_id") <= 3)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_approved_amount_60d",
        "policy_product",
        F.when(F.col("policy_product").isin("HOME", "TRAVEL"), "group_a").when(F.col("policy_product") == "AUTO", "group_b").otherwise("other").alias("policy_product_group"),
    )
)
