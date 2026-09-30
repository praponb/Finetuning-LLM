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
        "c.status",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

rollup_base = (
    matched
    .filter(F.col("premium") >= 2111.3)
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        "policy_id",
    )
    .agg(
        F.count(F.when(F.col("claim_type") != "collision", 1)).alias("n_claim_type_collision"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.max("claim_date").alias("last_claim_date"),
        F.sum(F.when(F.col("status").isNotNull(), F.col("claimed_amount")).otherwise(0)).alias("claimed_amount_status_is_not_null"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
    )
)

enriched = (
    rollup_base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.claim_date_month",
        "s.policy_id",
        "s.product_list",
        "s.last_claim_date",
        "s.claimed_amount_status_is_not_null",
        "s.claim_type_list",
        F.col("p.premium").alias("policy_premium"),
    )
)

per_label = (
    enriched
    .select(
        "claim_date_month",
        "policy_id",
        "last_claim_date",
        "claimed_amount_status_is_not_null",
        "claim_type_list",
        "policy_premium",
        F.explode("product_list").alias("product_list_item"),
    )
)

result = (
    per_label
    .select(
        "claim_date_month",
        "policy_id",
        "claimed_amount_status_is_not_null",
        "product_list_item",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_claim_date")).alias("days_since_last_claim_date"),
        F.date_add(F.col("last_claim_date"), 7).alias("last_claim_date_plus_7d"),
    )
)
