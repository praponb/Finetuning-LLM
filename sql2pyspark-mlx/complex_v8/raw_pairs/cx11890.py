from pyspark.sql import functions as F

in_window = (
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
        "c.adjuster_id",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

enriched_policy = (
    in_window.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.approved_amount",
        "s.product",
        "s.premium",
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
    )
)

per_group = (
    enriched_policy
    .groupBy(
        F.date_trunc("month", "policy_end_date").alias("policy_end_date_month"),
        "product",
    )
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_approved_amount"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.round(F.sum("policy_premium"), 2).alias("total_policy_premium"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("premium").alias("max_premium"),
        F.sum("approved_amount").alias("sum_approved_amount_check"),
    )
    .filter(F.col("sum_approved_amount_check") > 0.0)
    .drop("sum_approved_amount_check")
)

result = (
    per_group
    .groupingSets(
        [["distinct_policy_ids", "product", "distinct_ids"], ["distinct_policy_ids"], []],
        "distinct_policy_ids", "product", "distinct_ids",
    )
    .agg(F.countDistinct("product").alias("distinct_products"))
    .orderBy(
        F.col("distinct_policy_ids").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
        F.col("distinct_ids").asc_nulls_last(),
    )
)
