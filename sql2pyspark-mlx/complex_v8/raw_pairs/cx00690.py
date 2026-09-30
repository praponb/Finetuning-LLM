from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_greatest_approved = Window.partitionBy("status").orderBy("greatest_approved_amount_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_reported_at_month = Window.partitionBy("status", F.month("reported_at"))
w_status_by_greatest_approved_2 = Window.partitionBy("status").orderBy("greatest_approved_amount_claimed_amount", "id")
w_dedup = Window.partitionBy("status", "adjuster_team").orderBy("status_reported_at_month_min_min_claim_claimed_a", "id")

scoped = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2025-04-17").cast("date"))
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
        F.year(F.col("reported_at")).alias("reported_at_year"),
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
    )
)

with_stats = (
    scoped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.last_claim_date", "c.min_claim_claimed_amount")
)

ranked = (
    with_stats
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "greatest_claimed_amount_approved_amount",
        "last_claim_date",
        "min_claim_claimed_amount",
        F.min("min_claim_claimed_amount").over(w_status_by_greatest_approved).alias("min_min_claim_claimed_amount_so_far"),
        F.min("min_claim_claimed_amount").over(w_status_reported_at_month).alias("status_reported_at_month_min_min_claim_claimed_a"),
        (F.col("reported_at_year") - F.lag("reported_at_year").over(w_status_by_greatest_approved_2)).alias("reported_at_year_change"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status_reported_at_month), 2).alias("approved_amount_vs_status_reported_at_month_avg"),
        F.rank().over(w_status_by_greatest_approved_2).alias("greatest_approved_amount_claimed_amount_rank"),
    )
)

with_parent = (
    ranked.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

latest_per_key = (
    with_parent
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "greatest_claimed_amount_approved_amount",
        "min_claim_claimed_amount",
        "min_min_claim_claimed_amount_so_far",
        "status_reported_at_month_min_min_claim_claimed_a",
        "approved_amount_vs_status_reported_at_month_avg",
        "greatest_approved_amount_claimed_amount_rank",
        "adjuster_name",
        "adjuster_team",
    )
    .orderBy("min_min_claim_claimed_amount_so_far")
)
