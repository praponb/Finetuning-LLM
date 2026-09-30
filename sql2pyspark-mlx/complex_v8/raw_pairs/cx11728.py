from pyspark.sql import functions as F

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.approved_amount",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

filtered = (
    covered.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.product") == "TRAVEL") & (F.col("p.premium") > 2289.0), "left_semi")
)

filtered_2 = (
    filtered
    .crossJoin(filtered.agg(F.max("claimed_amount").alias("overall_max_claimed_amount")))
    .filter(F.col("claimed_amount") < F.col("overall_max_claimed_amount"))
    .drop("overall_max_claimed_amount")
)

result = (
    filtered_2
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "product",
        "premium",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        (F.col("claimed_amount") - F.col("premium")).alias("claimed_amount_minus_premium"),
    )
)
