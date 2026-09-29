from pyspark.sql import functions as F

joined_ranges = (
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
        "c.status",
        "p.product",
        "p.premium",
    )
)

filtered = (
    joined_ranges.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
    .filter(F.col("claimed_amount") > 18478.3)
)

summary = (
    filtered
    .filter(F.col("premium") > 2289.0)
    .groupBy("policy_id", "status", "claim_type")
    .agg(
        F.count("*").alias("row_count"),
        F.max("claimed_amount").alias("max_claimed_amount"),
        F.sum(F.when(F.col("product") == "AUTO", 1).otherwise(0)).alias("n_product_auto"),
        F.round(F.stddev("claimed_amount"), 2).alias("stddev_claimed_amount"),
    )
    .filter(F.col("max_claimed_amount") > 18364.1)
)

enriched = (
    summary.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.policy_id",
        "s.status",
        "s.claim_type",
        "s.row_count",
        "s.max_claimed_amount",
        "s.stddev_claimed_amount",
        F.col("p.status").alias("policy_status"),
        F.col("p.holder_id").alias("policy_holder_id"),
        "po.holder_name",
    )
)

result = (
    enriched
    .select(
        "policy_id",
        "status",
        "claim_type",
        "row_count",
        "max_claimed_amount",
        "stddev_claimed_amount",
        "policy_status",
        "policy_holder_id",
        F.concat_ws(" / ", F.col("status"), F.col("holder_name")).alias("status_holder_name_label"),
    )
    .orderBy("claim_type", "policy_id", "status")
    .limit(10)
)
