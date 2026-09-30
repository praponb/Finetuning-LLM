from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount_full = Window.orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_month = Window.partitionBy("reported_at_month")

base_claims = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.when(F.col("status").isin("APPROVED", "PAID"), "group_a").when(F.col("status") == "IN_REVIEW", "group_b").otherwise("other").alias("status_group"),
        F.month(F.col("reported_at")).alias("reported_at_month"),
    )
)

ranked = (
    base_claims
    .withColumn("second_reported_at_month", F.nth_value("reported_at_month", 2).over(w_all_by_claimed_amount_full))
    .withColumn("rows_in_reported_at_month", F.count(F.lit(1)).over(w_reported_at_month))
)

screened = (
    ranked.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") < 20880.4), "left_anti")
)

enriched = (
    screened.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.status_group",
        "s.reported_at_month",
        "s.second_reported_at_month",
        "s.rows_in_reported_at_month",
        F.col("p.start_date").alias("policy_start_date"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.reported_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")), "left_anti")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count("*").alias("claim_count_2"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
        "c.first_claim_date",
    )
)

result = (
    with_counts
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "second_reported_at_month",
        "rows_in_reported_at_month",
        "policy_start_date",
        "claim_count_2",
        "first_claim_date",
        F.datediff(F.col("first_claim_date"), F.col("policy_start_date")).alias("days_policy_start_date_to_first_claim_date"),
    )
    .orderBy(F.col("claimed_amount").desc_nulls_first())
)
