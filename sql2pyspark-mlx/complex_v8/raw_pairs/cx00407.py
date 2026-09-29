from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_adjuster_id_status_by_approv = Window.partitionBy("adjuster_id", "status").orderBy(F.col("approved_amount").asc_nulls_last(), "id")
w_adjuster_id_status_by_status = Window.partitionBy("adjuster_id", "status").orderBy("status_max_approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc_nulls_first(), "id")

cleaned_claims = (
    df_claims
    .filter((F.col("adjuster_id") >= 7) & (F.col("claim_type").isNotNull()))
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
        F.round(F.col("claimed_amount") * 0.9, 2).alias("claimed_amount_adj"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

windowed = (
    cleaned_claims
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        "claimed_amount_adj",
        F.max("approved_amount").over(w_status).alias("status_max_approved_amount"),
        F.lag("claim_date").over(w_adjuster_id_status_by_approv).alias("prev_claim_date"),
        F.lead("reported_at").over(w_adjuster_id_status_by_approv).alias("next_reported_at"),
    )
)

derived = (
    windowed
    .filter(F.col("reported_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp"))
    .select(
        "id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        "status_max_approved_amount",
        "prev_claim_date",
        "next_reported_at",
        (F.floor(F.col("claimed_amount_adj") / 10000) * 10000).alias("claimed_amount_adj_bucket"),
    )
)

screened = (
    derived
    .crossJoin(derived.agg(F.avg("approved_amount").alias("overall_avg_approved_amount")))
    .filter(F.col("approved_amount") > F.col("overall_avg_approved_amount"))
    .drop("overall_avg_approved_amount")
)

with_trends = (
    screened
    .select(
        "*",
        F.last("reported_at", ignorenulls=True).over(w_adjuster_id_status_by_status).alias("last_known_reported_at"),
        F.round(F.col("claimed_amount_adj_bucket") / F.nullif(F.sum("claimed_amount_adj_bucket").over(w_status_adjuster_id), F.lit(0)), 4).alias("claimed_amount_adj_bucket_share_of_status_adjust"),
        F.round(F.col("status_max_approved_amount") - F.avg("status_max_approved_amount").over(w_status_adjuster_id), 2).alias("status_max_approved_amount_vs_status_adjuster_id"),
        F.ntile(3).over(w_all_by_approved_amount).alias("approved_amount_tercile"),
    )
)

per_group = (
    with_trends
    .groupBy(F.year("reported_at").alias("reported_at_year"))
    .agg(
        F.count(F.when(F.col("status") == "IN_REVIEW", 1)).alias("n_status_in_review"),
        F.max("claimed_amount_adj_bucket").alias("max_claimed_amount_adj_bucket"),
        F.percentile_approx("status_max_approved_amount_vs_status_adjuster_id", 0.5).alias("median_status_max_approved_amount_vs_status_adju"),
    )
)

without_matches = (
    per_group
    .crossJoin(per_group.agg(F.avg("max_claimed_amount_adj_bucket").alias("overall_avg_max_claimed_amount_adj_bucket")))
    .filter(F.col("max_claimed_amount_adj_bucket") > F.col("overall_avg_max_claimed_amount_adj_bucket"))
    .drop("overall_avg_max_claimed_amount_adj_bucket")
)

result = (
    without_matches
    .select(
        "reported_at_year",
        "n_status_in_review",
        "max_claimed_amount_adj_bucket",
        "median_status_max_approved_amount_vs_status_adju",
        F.when(F.col("n_status_in_review") >= 8, "A").when(F.col("n_status_in_review") >= 4, "B").otherwise("C").alias("n_status_in_review_tier"),
    )
    .orderBy("n_status_in_review")
)
