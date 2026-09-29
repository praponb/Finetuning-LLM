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
        "c.status",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

rollup_base = (
    matched
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"), "status")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("premium").alias("min_premium"),
        F.min("claimed_amount").alias("min_claimed_amount"),
        F.sum("premium").alias("total_premium"),
        F.countDistinct("status").alias("distinct_status"),
    )
)

qualified = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("distinct_ids").alias("overall_avg_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_avg_distinct_ids"))
    .drop("overall_avg_distinct_ids")
)

result = (
    qualified
    .select("claim_date_month", "status", "distinct_status")
    .orderBy(F.col("status").asc(), "claim_date_month")
    .limit(20)
)
