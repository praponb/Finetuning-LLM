from pyspark.sql import functions as F

matched = (
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
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

grouped = (
    matched
    .groupBy("adjuster_id")
    .agg(
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.round(F.sum("premium") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("premium_to_claimed_amount_ratio"),
        F.max("claimed_amount").alias("max_claimed_amount"),
    )
)

tagged = (
    grouped
    .filter(F.col("premium_to_claimed_amount_ratio") <= 8.5)
    .select(
        "*",
        (F.col("max_claimed_amount") - F.col("avg_claimed_amount")).alias("max_claimed_amount_minus_avg_claimed_amount"),
        F.round(F.col("avg_claimed_amount") * 1.07, 2).alias("avg_claimed_amount_adj"),
        F.greatest(F.col("max_claimed_amount"), F.col("avg_claimed_amount")).alias("greatest_max_claimed_amount_avg_claimed_amount"),
    )
)

result = (
    tagged
    .select(
        "adjuster_id",
        "avg_claimed_amount",
        "premium_to_claimed_amount_ratio",
        "max_claimed_amount",
        "avg_claimed_amount_adj",
        "greatest_max_claimed_amount_avg_claimed_amount",
    )
    .orderBy(F.col("premium_to_claimed_amount_ratio").desc())
)
