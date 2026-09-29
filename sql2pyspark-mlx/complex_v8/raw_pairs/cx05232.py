from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 3465.4).alias("premium_distance"),
        F.date_trunc("month", F.col("start_date")).alias("start_date_month"),
        F.date_format(F.col("end_date"), "MMM yyyy").alias("end_date_label"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count(F.when(F.col("claim_type") == "theft", 1)).alias("claim_claim_type_count"),
    )
)

summarized_rows = (
    scoped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.avg_claim_claimed_amount",
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

scored = (
    summarized_rows
    .select(
        "*",
        F.concat_ws("-", F.col("end_date_label"), F.col("status")).alias("end_date_label_status_label"),
        F.round(F.col("premium_distance") * 1.07, 2).alias("premium_distance_adj"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

joined_holder = (
    scored.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.premium_distance",
        "s.start_date_month",
        "s.end_date_label",
        "s.avg_claim_claimed_amount",
        "s.last_claim_reported_at",
        "s.claim_claim_type_count",
        "s.premium_distance_adj",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

labeled = (
    joined_holder
    .select(
        "*",
        F.add_months(F.col("end_date"), 1).alias("end_date_plus_1m"),
        F.when(F.col("premium_distance") >= 3750.0, "large").when(F.col("premium_distance") >= 1750.0, "medium").otherwise("small").alias("premium_distance_tier"),
        F.round(F.col("avg_claim_claimed_amount") / F.nullif(F.col("holder_risk_score"), F.lit(0)), 3).alias("avg_claim_claimed_amount_per_holder_risk_score"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.min("claimed_amount").alias("min_claim_claimed_amount_2"),
    )
)

summarized_rows_2 = (
    labeled.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.min_claim_claimed_amount",
        "c.min_claim_claimed_amount_2",
    )
)

result = (
    summarized_rows_2
    .groupingSets(
        [["claim_count", "holder_id", "claim_claim_type_count"], ["claim_count"], []],
        "claim_count", "holder_id", "claim_claim_type_count",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("status").alias("distinct_status"),
    )
)
