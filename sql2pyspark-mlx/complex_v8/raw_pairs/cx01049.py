from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_date_month = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date"))
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc(), "id")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_adjuster_id_by_claimed_amoun_2 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_claim_date = Window.partitionBy("adjuster_id").orderBy("claim_date", "id")
w_all_by_claim_date_full = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_claimed_amoun_3 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_claim_date_cume_dist = Window.orderBy("claim_date_cume_dist", "id")
w_all_by_claim_date_2 = Window.orderBy("claim_date", "id")
w_all_by_claim_date_cume_dist_2 = Window.orderBy("claim_date_cume_dist", "id").rowsBetween(-2, -1)
w_all_by_claim_date_cume_dist_3 = Window.orderBy("claim_date_cume_dist")

cleaned = (
    df_claims
    .filter((F.col("claimed_amount") > 3703.0) & (F.col("approved_amount") <= 21750.2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.count(F.lit(1)).over(w_policy_id_claim_date_month).alias("rows_in_policy_id_claim_date_month"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_adjuster_id_by_claimed_amoun))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id_claim_date_month), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id_claim_date_mon"),
        F.dense_rank().over(w_all_by_claim_date_ties).alias("claim_date_dense_rank"),
        F.max("claimed_amount").over(w_policy_id_claim_date_month).alias("policy_id_claim_date_month_max_claimed_amount"),
        F.lag("claimed_amount", 1, 0).over(w_all_by_claim_date).alias("prev_claimed_amount"),
        F.count(F.lit(1)).over(w_adjuster_id_by_claimed_amoun_2).alias("running_count"),
        F.lag("claim_date").over(w_adjuster_id_by_claimed_amoun).alias("prev_claim_date"),
        F.round(F.cume_dist().over(w_all_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.rank().over(w_adjuster_id_by_claim_date).alias("claim_date_rank"),
        F.nth_value("claimed_amount", 2).over(w_all_by_claim_date_full).alias("second_claimed_amount"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_by_claimed_amoun_3), 2).alias("claimed_amount_avg_7"),
        F.rank().over(w_all_by_claim_date_ties).alias("claim_date_rank_2"),
    )
)

top_rows = with_windows.filter(F.col("claim_date_dense_rank") == 1)

with_windows_2 = (
    top_rows
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        "claim_date_dense_rank",
        "prev_claimed_amount",
        "running_count",
        "claim_date_rank",
        "second_claimed_amount",
        "claimed_amount_avg_7",
        "claim_date_rank_2",
        F.ntile(10).over(w_all_by_claim_date_cume_dist).alias("claim_date_cume_dist_decile"),
        (F.col("claim_date_rank") - F.lag("claim_date_rank").over(w_all_by_claim_date_2)).alias("claim_date_rank_change"),
        F.lead("claim_date_cume_dist", 1, 0).over(w_all_by_claim_date_2).alias("next_claim_date_cume_dist"),
        F.sum("prev_claimed_amount").over(w_all_by_claim_date_cume_dist_2).alias("prev_claimed_amount_sum_2_prior"),
        F.round(F.percent_rank().over(w_all_by_claim_date_cume_dist_3), 3).alias("claim_date_cume_dist_percent_rank"),
        F.rank().over(w_all_by_claim_date_cume_dist).alias("claim_date_cume_dist_rank"),
    )
)

stats = (
    with_windows_2
    .groupBy("adjuster_id")
    .agg(
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.min("claim_date").alias("first_claim_date"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("claim_date_rank_change", 0.5).alias("median_claim_date_rank_change"),
        F.sum("claimed_amount_avg_7").alias("sum_claimed_amount_avg_7_check"),
    )
    .filter(F.col("sum_claimed_amount_avg_7_check") > 17565.0)
    .drop("sum_claimed_amount_avg_7_check")
)

result = (
    stats
    .select(
        "adjuster_id",
        "first_claim_date",
        "distinct_ids",
        "median_claim_date_rank_change",
        F.when(F.col("claim_date_span_days") >= 560, "HIGH").when(F.col("claim_date_span_days") >= 245, "MEDIUM").otherwise("LOW").alias("claim_date_span_days_tier"),
    )
    .orderBy(F.col("adjuster_id").asc())
    .limit(10)
)
