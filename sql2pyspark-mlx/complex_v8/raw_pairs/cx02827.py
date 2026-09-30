from pyspark.sql import functions as F

matched = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 180)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_180d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_180d"),
    )
)

tagged = (
    matched
    .filter(F.col("prior_rows_180d") < 7)
    .select("*", F.year(F.col("claim_date")).alias("claim_date_year"))
)

with_details = (
    tagged.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.premium") >= 1042.8)
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_180d",
        "s.prior_claimed_amount_180d",
        "s.claim_date_year",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
    )
)

result = (
    with_details
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_180d",
        "prior_claimed_amount_180d",
        "claim_date_year",
        "policy_premium",
    )
)
