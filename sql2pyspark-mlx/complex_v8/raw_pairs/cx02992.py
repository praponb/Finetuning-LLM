from pyspark.sql import functions as F

in_window = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 30)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_30d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_30d"),
    )
)

screened = (
    in_window.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date") >= F.lit("2024-04-15").cast("date")), "left_semi")
)

result = (
    screened
    .groupingSets(
        [["policy_id", "prior_rows_30d"], ["policy_id"], ["prior_rows_30d"], []],
        "policy_id", "prior_rows_30d",
    )
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("prior_rows_30d").asc_nulls_last(),
    )
)
