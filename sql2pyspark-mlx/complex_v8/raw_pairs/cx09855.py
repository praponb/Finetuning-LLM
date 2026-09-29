from pyspark.sql import functions as F

in_window = (
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

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("status").isin("APPROVED", "OPEN"), 1)).alias("claim_status_count"),
        F.count(F.when(F.col("status") == "IN_REVIEW", 1)).alias("claim_status_count_2"),
    )
)

with_counts = (
    in_window.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        F.coalesce(F.col("c.claim_status_count_2"), F.lit(0)).alias("claim_status_count_2"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_180d",
        "prior_claimed_amount_180d",
        "claim_status_count",
        "claim_status_count_2",
    )
    .orderBy(F.col("claim_date").desc_nulls_first())
)
