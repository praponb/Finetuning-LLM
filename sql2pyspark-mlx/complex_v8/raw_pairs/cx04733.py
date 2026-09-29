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
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

with_parent_policy = (
    matched.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .filter(F.col("p.holder_id").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.product",
        "s.premium",
        F.col("p.holder_id").alias("policy_holder_id"),
        "po.holder_name",
        F.col("po.joined_on").alias("holder_joined_on"),
    )
)

stats = (
    with_parent_policy
    .groupBy(
        "policy_holder_id",
        "product",
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
    )
    .agg(
        F.countDistinct("product").alias("distinct_products"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("premium") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("premium_to_claimed_amount_ratio"),
        F.countDistinct("policy_holder_id").alias("distinct_policy_holder_ids"),
        F.count("*").alias("cnt"),
    )
)

annotated = (
    stats
    .filter(F.col("premium_to_claimed_amount_ratio") > 4.0)
    .select(
        "policy_holder_id",
        "product",
        "claim_date_month",
        "status_list",
        "cnt",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.dayofweek(F.col("claim_date_month")).alias("claim_date_month_dayofweek"),
    )
)

result = (
    annotated
    .select(
        "policy_holder_id",
        "product",
        "cnt",
        "claim_date_month_dayofweek",
    )
    .unpivot(
        ids=["policy_holder_id", "product"],
        values=["cnt", "claim_date_month_dayofweek"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("policy_holder_id", "product", "field")
)
