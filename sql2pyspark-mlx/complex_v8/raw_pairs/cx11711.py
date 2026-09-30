from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_reported_at = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc(), "id")
w_claimed_amount_tier_adjuster = Window.partitionBy("claimed_amount_tier", "adjuster_id").orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_all_by_approved_amount_ties = Window.orderBy("approved_amount")
w_status_by_approved_amount_ti = Window.partitionBy("status").orderBy(F.col("approved_amount").desc())
w_claim_type_by_reported_at_ti = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc())
w_policy_id_claim_type = Window.partitionBy("policy_id", "claim_type")
w_claimed_amount_tier_adjuster_2 = Window.partitionBy("claimed_amount_tier", "adjuster_id").orderBy(F.col("approved_amount").desc_nulls_first())
w_claim_type_by_reported_at_fu = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prev_approved_amount = Window.orderBy(F.col("prev_approved_amount").desc(), "id")
w_all_by_approved_amount_cume = Window.orderBy(F.col("approved_amount_cume_dist_2").desc(), "id")
w_all_by_reported_at_percent_r = Window.orderBy("reported_at_percent_rank", "id")
w_prev2_reported_at_quarter_by = Window.partitionBy("prev2_reported_at_quarter").orderBy("approved_amount", "id")
w_prev2_reported_at_quarter_di = Window.partitionBy("prev2_reported_at_quarter", "distinct_adjuster_id_in_policy_id_claim_type")
w_all_by_prev_approved_amount_2 = Window.orderBy(F.col("prev_approved_amount").desc())

cleaned = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-03-16").cast("date")) & (F.col("claim_type") == "flood"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("claimed_amount") >= 38909.4, "A").when(F.col("claimed_amount") >= 14019.8, "B").otherwise("C").alias("claimed_amount_tier"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "policy_id",
        "approved_amount",
        F.lag("reported_at", 2).over(w_claim_type_by_reported_at).alias("prev2_reported_at"),
        F.lag("approved_amount", 1, 0).over(w_claimed_amount_tier_adjuster).alias("prev_approved_amount"),
        F.round(F.cume_dist().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_cume_dist"),
        F.round(F.cume_dist().over(w_status_by_approved_amount_ti), 3).alias("approved_amount_cume_dist_2"),
        F.round(F.percent_rank().over(w_claim_type_by_reported_at_ti), 3).alias("reported_at_percent_rank"),
        F.count(F.lit(1)).over(w_policy_id_claim_type).alias("rows_in_policy_id_claim_type"),
        F.dense_rank().over(w_claimed_amount_tier_adjuster_2).alias("approved_amount_dense_rank"),
        F.lag("approved_amount").over(w_claimed_amount_tier_adjuster).alias("prev_approved_amount_2"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id_claim_type)).alias("distinct_adjuster_id_in_policy_id_claim_type"),
        F.nth_value("approved_amount", 2).over(w_claim_type_by_reported_at_fu).alias("second_approved_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id_claim_type), 2).alias("approved_amount_vs_policy_id_claim_type_avg"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "approved_amount",
        "prev2_reported_at",
        "prev_approved_amount",
        "approved_amount_cume_dist",
        "approved_amount_cume_dist_2",
        "reported_at_percent_rank",
        "rows_in_policy_id_claim_type",
        "approved_amount_dense_rank",
        "distinct_adjuster_id_in_policy_id_claim_type",
        "approved_amount_vs_policy_id_claim_type_avg",
        F.quarter(F.col("prev2_reported_at")).alias("prev2_reported_at_quarter"),
        F.abs(F.col("second_approved_amount") - 4234.9).alias("second_approved_amount_distance"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_prev_approved_amount), 3).alias("prev_approved_amount_cume_dist"),
        F.rank().over(w_all_by_approved_amount_cume).alias("approved_amount_cume_dist_2_rank"),
        F.ntile(5).over(w_all_by_reported_at_percent_r).alias("reported_at_percent_rank_quintile"),
        F.ntile(4).over(w_prev2_reported_at_quarter_by).alias("approved_amount_quartile"),
        F.min("prev2_reported_at_quarter").over(w_prev2_reported_at_quarter_di).alias("prev2_reported_at_quarter_distinct_adjuster_id_i"),
        F.rank().over(w_all_by_prev_approved_amount_2).alias("prev_approved_amount_rank"),
    )
)

result = (
    ranked
    .filter(F.col("second_approved_amount_distance") < 40000.0)
    .select(
        "id",
        "approved_amount",
        "prev2_reported_at",
        "prev_approved_amount",
        "rows_in_policy_id_claim_type",
        "approved_amount_dense_rank",
        "distinct_adjuster_id_in_policy_id_claim_type",
        "approved_amount_vs_policy_id_claim_type_avg",
        "prev_approved_amount_cume_dist",
        "approved_amount_cume_dist_2_rank",
        "approved_amount_quartile",
        "prev_approved_amount_rank",
        F.date_format(F.col("prev2_reported_at"), "yyyy-MM-dd").alias("prev2_reported_at_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("prev2_reported_at")).alias("days_since_prev2_reported_at"),
    )
    .orderBy(
        F.col("distinct_adjuster_id_in_policy_id_claim_type").asc(),
        F.col("prev_approved_amount_rank").desc_nulls_first(),
    )
)
