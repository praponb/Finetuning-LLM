from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_month_by_reporte = Window.partitionBy(F.month("reported_at")).orderBy("reported_at", "id")
w_reported_at_month_by_reporte_2 = Window.partitionBy(F.month("reported_at")).orderBy("reported_at")
w_status_by_approved_amount_ti = Window.partitionBy("status").orderBy("approved_amount")
w_adjuster_id_claim_type_or_un = Window.partitionBy("adjuster_id", "claim_type_or_unknown").orderBy("reported_at_rank", "id").rowsBetween(-3, -1)
w_policy_id_reported_at_rank_b = Window.partitionBy("policy_id", "reported_at_rank").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_reported_at_rank_b_2 = Window.partitionBy("policy_id", "reported_at_rank").orderBy("approved_amount", "id").rowsBetween(-3, -1)
w_reported_at_month_claim_type = Window.partitionBy(F.month("reported_at"), "claim_type_or_unknown")
w_policy_id_reported_at_rank_b_3 = Window.partitionBy("policy_id", "reported_at_rank").orderBy("approved_amount", "id")

scoped = (
    df_claims
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.when(F.col("claimed_amount") >= 41878.5, "large").when(F.col("claimed_amount") >= 19337.6, "medium").otherwise("small").alias("claimed_amount_tier"),
        F.concat_ws(" / ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.round(F.percent_rank().over(w_reported_at_month_by_reporte), 3).alias("reported_at_percent_rank"),
        F.rank().over(w_reported_at_month_by_reporte_2).alias("reported_at_rank"),
        F.round(F.cume_dist().over(w_status_by_approved_amount_ti), 3).alias("approved_amount_cume_dist"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        "claim_type_or_unknown",
        "claimed_amount_tier",
        "reported_at_rank",
        "approved_amount_cume_dist",
        F.max("reported_at_percent_rank").over(w_adjuster_id_claim_type_or_un).alias("reported_at_percent_rank_max_3_prior"),
        F.nth_value("reported_at", 2).over(w_policy_id_reported_at_rank_b).alias("second_reported_at"),
        F.round(F.avg("approved_amount").over(w_policy_id_reported_at_rank_b_2), 2).alias("approved_amount_avg_3_prior"),
        F.round(F.avg("approved_amount_cume_dist").over(w_reported_at_month_claim_type), 2).alias("reported_at_month_claim_type_or_unknown_avg_appr"),
        F.lag("reported_at_rank").over(w_policy_id_reported_at_rank_b_3).alias("prev_reported_at_rank"),
    )
)

adjuster_id_stats = (
    windowed
    .groupBy("adjuster_id")
    .agg(F.avg("reported_at_rank").alias("avg_reported_at_rank"))
)

outliers = (
    windowed.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.reported_at_rank") > F.col("g.avg_reported_at_rank"))
    .select(
        "s.*",
        "g.avg_reported_at_rank",
        F.round(F.col("s.reported_at_rank") - F.col("g.avg_reported_at_rank"), 2).alias("reported_at_rank_minus_avg"),
    )
)

labeled = (
    outliers
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount_tier",
        "reported_at_rank",
        "approved_amount_cume_dist",
        "reported_at_percent_rank_max_3_prior",
        "reported_at_month_claim_type_or_unknown_avg_appr",
        "avg_reported_at_rank",
        "reported_at_rank_minus_avg",
        F.lower(F.regexp_replace(F.trim(F.col("claimed_amount_tier")), " ", "_")).alias("claimed_amount_tier_slug"),
    )
)

result = (
    labeled
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount_tier",
        "reported_at_rank",
        "approved_amount_cume_dist",
        "avg_reported_at_rank",
        "reported_at_rank_minus_avg",
        "claimed_amount_tier_slug",
        F.initcap(F.trim(F.col("claimed_amount_tier_slug"))).alias("claimed_amount_tier_slug_clean"),
    )
    .orderBy(F.col("reported_at_rank").asc(), F.col("policy_id").desc())
)
