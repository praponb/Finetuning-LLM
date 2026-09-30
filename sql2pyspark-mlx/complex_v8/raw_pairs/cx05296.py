from pyspark.sql import functions as F

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.status",
        "p.product",
        "p.premium",
    )
)

summary = (
    covered
    .groupBy("product", "status", "claim_type")
    .agg(
        F.min("claimed_amount").alias("min_claimed_amount"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.sum(F.when(F.col("claim_type").isin("delay", "theft", "flood"), 1).otherwise(0)).alias("n_claim_type_in_delay_theft_fl"),
    )
)

result = (
    summary
    .filter(F.col("avg_claimed_amount") < 42515.0)
    .select(
        "product",
        "status",
        "min_claimed_amount",
        "avg_claimed_amount",
        "n_claim_type_in_delay_theft_fl",
    )
    .orderBy(F.col("product").desc(), F.col("status").asc())
)
