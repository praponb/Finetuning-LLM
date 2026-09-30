from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_hour_ties = Window.orderBy("reported_at_hour")
w_claim_date_month = Window.partitionBy(F.date_trunc("month", "claim_date"))
w_claim_type_claim_count_by_cl = Window.partitionBy("claim_type", "claim_count").orderBy(F.col("claim_count_2").desc(), "id")
w_all_by_reported_at_hour_full = Window.orderBy("reported_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-03-08 14:00:00").cast("timestamp"))
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
    )
)

qualified = (
    valid.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
    .filter(~F.col("status").isin("REJECTED", "IN_REVIEW"))
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

with_counts = (
    qualified.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

tagged = (
    with_counts
    .filter(F.col("approved_amount") >= 0.0)
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claim_count",
        "claim_count_2",
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.hour(F.col("reported_at")).alias("reported_at_hour"),
    )
)

with_windows = (
    tagged
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_reported_at_hour_ties), 3).alias("reported_at_hour_cume_dist"),
        F.round(F.avg("approved_amount").over(w_claim_date_month), 2).alias("claim_date_month_avg_approved_amount"),
        F.max("claim_count").over(w_claim_date_month).alias("claim_date_month_max_claim_count"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_date_month), 2).alias("claimed_amount_vs_claim_date_month_avg"),
        F.lag("claimed_amount").over(w_claim_type_claim_count_by_cl).alias("prev_claimed_amount"),
        F.first("claimed_amount").over(w_all_by_reported_at_hour_full).alias("first_claimed_amount"),
    )
)

claim_type_stats = (
    with_windows
    .groupBy("claim_type")
    .agg(F.avg("approved_amount").alias("avg_approved_amount"))
)

compared = (
    with_windows.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.approved_amount") > F.col("g.avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.avg_approved_amount"), 2).alias("approved_amount_minus_avg"),
    )
)

tagged_2 = (
    compared
    .filter(F.col("claim_type_prefix").isNotNull())
    .select(
        "*",
        F.date_add(F.col("claim_date"), 90).alias("claim_date_plus_90d"),
        F.substring(F.trim(F.col("status_claim_type_label")), 1, 2).alias("status_claim_type_label_prefix"),
    )
)

result = (
    tagged_2
    .select(
        "id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "claim_count",
        "claim_count_2",
        "claim_type_prefix",
        "status_claim_type_label",
        "claim_date_month_avg_approved_amount",
        "claim_date_month_max_claim_count",
        "claimed_amount_vs_claim_date_month_avg",
        "prev_claimed_amount",
        "first_claimed_amount",
        "avg_approved_amount",
        "status_claim_type_label_prefix",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)
