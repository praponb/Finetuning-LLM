from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_n_status_in_re = Window.partitionBy("claim_type").orderBy("n_status_in_review_tier_len")
w_claim_type_by_n_status_in_re_2 = Window.partitionBy("claim_type").orderBy("n_status_in_review_tier_len", "claim_type").rowsBetween(-1, -1)
w_n_status_in_review_tier_len = Window.partitionBy("n_status_in_review_tier_len")

filtered_claims = (
    df_claims
    .filter(F.col("claim_date").between(F.lit("2024-02-19").cast("date"), F.lit("2025-04-20").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
        F.concat_ws(": ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

summary = (
    filtered_claims
    .groupBy("claim_type")
    .agg(
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("n_claim_type_is_not_null"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") != "IN_REVIEW", 1)).alias("n_status_in_review"),
        F.round(F.avg("claimed_amount_plus_approved_amount"), 2).alias("avg_claimed_amount_plus_approved_amount"),
        F.sum("claimed_amount_plus_approved_amount").alias("total_claimed_amount_plus_approved_amount"),
        F.sum("claimed_amount").alias("sum_claimed_amount_check"),
    )
    .filter(F.col("sum_claimed_amount_check") > 4234.9)
    .drop("sum_claimed_amount_check")
)

tagged = (
    summary
    .select(
        "claim_type",
        "n_claim_type_is_not_null",
        "n_status_in_review",
        "avg_claimed_amount_plus_approved_amount",
        "total_claimed_amount_plus_approved_amount",
        F.least(F.col("n_status_in_review"), F.col("n_claim_type_is_not_null")).alias("least_n_status_in_review_n_claim_type_is_not_nul"),
        F.abs(F.col("n_status_in_review") - 6).alias("n_status_in_review_distance"),
    )
)

labeled = (
    tagged
    .filter(F.col("total_claimed_amount_plus_approved_amount") >= 180000.0)
    .select(
        "claim_type",
        "total_claimed_amount_plus_approved_amount",
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.coalesce(F.col("n_status_in_review"), F.lit(0)).alias("n_status_in_review_filled"),
        F.when(F.col("n_status_in_review") >= 8, "A").when(F.col("n_status_in_review") >= 4, "B").otherwise("C").alias("n_status_in_review_tier"),
    )
)

derived = (
    labeled
    .select(
        "claim_type",
        "total_claimed_amount_plus_approved_amount",
        "claim_type_clean",
        "n_status_in_review_filled",
        F.length(F.trim(F.col("n_status_in_review_tier"))).alias("n_status_in_review_tier_len"),
    )
)

ranked = (
    derived
    .select(
        "*",
        F.round(F.cume_dist().over(w_claim_type_by_n_status_in_re), 3).alias("n_status_in_review_tier_len_cume_dist"),
        F.round(F.percent_rank().over(w_claim_type_by_n_status_in_re), 3).alias("n_status_in_review_tier_len_percent_rank"),
        F.max("n_status_in_review_tier_len").over(w_claim_type_by_n_status_in_re_2).alias("n_status_in_review_tier_len_max_1_prior"),
        F.round(F.col("n_status_in_review_tier_len") - F.avg("n_status_in_review_tier_len").over(w_n_status_in_review_tier_len), 2).alias("n_status_in_review_tier_len_vs_n_status_in_revie"),
        F.count(F.lit(1)).over(w_n_status_in_review_tier_len).alias("rows_in_n_status_in_review_tier_len"),
    )
)

result = (
    ranked
    .select(
        "total_claimed_amount_plus_approved_amount",
        "n_status_in_review_tier_len_percent_rank",
        "n_status_in_review_tier_len_vs_n_status_in_revie",
    )
    .distinct()
    .orderBy(
        F.col("total_claimed_amount_plus_approved_amount").asc(),
        F.col("n_status_in_review_tier_len_percent_rank").desc(),
    )
)
