from pyspark.sql import functions as F

matched = (
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
        "c.approved_amount",
        "c.status",
        "p.product",
        "p.premium",
    )
)

totals = (
    matched
    .filter(F.col("premium") < 3719.7)
    .groupBy("status")
    .agg(
        F.countDistinct("product").alias("distinct_products"),
        F.round(F.sum("approved_amount") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("approved_amount_to_claimed_amount_ratio"),
        F.count(F.when(F.col("status").isin("APPROVED", "PAID", "OPEN"), 1)).alias("n_status_in_approved_paid_open"),
        F.round(F.stddev("approved_amount"), 2).alias("stddev_approved_amount"),
        F.max("premium").alias("max_premium_check"),
    )
    .filter(F.col("max_premium_check") > 1042.8)
    .drop("max_premium_check")
)

result = totals.select("status", "n_status_in_approved_paid_open")
