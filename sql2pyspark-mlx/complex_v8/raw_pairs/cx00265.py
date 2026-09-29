from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id", "status").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())

recent = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-03-02 13:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.min("claimed_amount").alias("min_claim_claimed_amount"))
)

with_stats = (
    recent.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.min_claim_claimed_amount")
)

scored = (
    with_stats
    .filter(F.col("claim_date") >= F.lit("2024-06-13").cast("date"))
    .select(
        "*",
        F.round(F.col("approved_amount") * 1.5, 2).alias("approved_amount_adj"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date_month_start")).alias("days_since_claim_date_month_start"),
    )
)

combined_details_policy = (
    scored.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.status").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.claim_date_month_start",
        "s.min_claim_claimed_amount",
        "s.approved_amount_adj",
        "s.approved_amount_filled",
        "s.days_since_claim_date_month_start",
        F.col("p.product").alias("policy_product"),
        F.col("p.status").alias("policy_status"),
    )
)

unique_rows = (
    combined_details_policy
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "claim_date_month_start",
        "min_claim_claimed_amount",
        "approved_amount_adj",
        "approved_amount_filled",
        "days_since_claim_date_month_start",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.when(F.col("status").isin("REJECTED", "APPROVED"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
    )
    .orderBy(
        F.col("days_since_claim_date_month_start").desc_nulls_first(),
        F.col("claimed_amount").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
