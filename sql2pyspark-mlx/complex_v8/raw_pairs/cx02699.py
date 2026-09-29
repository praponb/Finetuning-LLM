from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-03-21").cast("date")) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
    )
)

with_children_claims = (
    base_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

with_activity = (
    with_children_claims
    .crossJoin(with_children_claims.agg(F.min("claim_approved_amount").alias("overall_min_claim_approved_amount")))
    .filter(F.col("claim_approved_amount") > F.col("overall_min_claim_approved_amount"))
    .drop("overall_min_claim_approved_amount")
)

product_stats = (
    with_activity
    .groupBy("product")
    .agg(F.avg("claim_approved_amount").alias("avg_claim_approved_amount"))
)

above_average = (
    with_activity.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.claim_approved_amount") > F.col("g.avg_claim_approved_amount"))
    .select(
        "s.*",
        "g.avg_claim_approved_amount",
        F.round(F.col("s.claim_approved_amount") - F.col("g.avg_claim_approved_amount"), 2).alias("claim_approved_amount_minus_avg"),
    )
)

result = (
    above_average
    .groupingSets(
        [["holder_id", "product", "claim_id"], ["holder_id"], ["product"]],
        "holder_id", "product", "claim_id",
    )
    .agg(F.count("*").alias("cnt"))
    .select(
        "holder_id",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "claim_id",
        "cnt",
    )
)
