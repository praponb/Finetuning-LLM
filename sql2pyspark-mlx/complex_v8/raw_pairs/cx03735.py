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
        "c.status",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

screened = (
    covered
    .crossJoin(covered.agg(F.max("claimed_amount").alias("overall_max_claimed_amount")))
    .filter(F.col("claimed_amount") < F.col("overall_max_claimed_amount"))
    .drop("overall_max_claimed_amount")
)

result = (
    screened
    .filter(F.col("product").isin("TRAVEL", "HEALTH"))
    .select(
        "id",
        "policy_id",
        "status",
        "approved_amount",
        "product",
        "premium",
        F.concat_ws("-", F.col("product"), F.col("status")).alias("product_status_label"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
