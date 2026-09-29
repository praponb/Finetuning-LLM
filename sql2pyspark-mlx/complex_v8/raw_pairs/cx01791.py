from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_adjuster_id_by_claime = Window.partitionBy("status", "adjuster_id").orderBy("claimed_amount_adj", "id")
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_adjuster_id_status_by_report = Window.partitionBy("adjuster_id", "status").orderBy("reported_at", "id")
w_status_policy_id = Window.partitionBy("status", "policy_id")
w_status_adjuster_id_by_claime_2 = Window.partitionBy("status", "adjuster_id").orderBy("claimed_amount_adj", "id").rowsBetween(-3, Window.currentRow)
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc_nulls_first(), "id")

base_claims = (
    df_claims
    .filter((F.col("claim_type") == "delay") | (F.col("adjuster_id") >= 6))
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
        F.round(F.col("claimed_amount") * 1.07, 2).alias("claimed_amount_adj"),
    )
)

windowed = (
    base_claims
    .select(
        "*",
        F.last("approved_amount", ignorenulls=True).over(w_all_by_reported_at_running).alias("last_known_approved_amount"),
        F.round(F.percent_rank().over(w_status_adjuster_id_by_claime), 3).alias("claimed_amount_adj_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_reported_at_ties), 3).alias("reported_at_percent_rank"),
        F.max("approved_amount").over(w_status_adjuster_id).alias("status_adjuster_id_max_approved_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status_adjuster_id), 2).alias("approved_amount_vs_status_adjuster_id_avg"),
        F.rank().over(w_adjuster_id_status_by_report).alias("reported_at_rank"),
        F.sum("claimed_amount").over(w_status_policy_id).alias("status_policy_id_sum_claimed_amount"),
        F.max("claimed_amount").over(w_status_adjuster_id).alias("status_adjuster_id_max_claimed_amount"),
        F.sum("claimed_amount").over(w_status_adjuster_id).alias("status_adjuster_id_sum_claimed_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_status_policy_id"),
        F.round(F.avg("claimed_amount_adj").over(w_status_adjuster_id_by_claime_2), 2).alias("claimed_amount_adj_avg_4"),
        F.count(F.lit(1)).over(w_status_adjuster_id).alias("rows_in_status_adjuster_id"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_status_policy_id"),
        F.lead("approved_amount", 2).over(w_status_adjuster_id_by_claime).alias("next2_approved_amount"),
        F.ntile(10).over(w_all_by_claim_date).alias("claim_date_decile"),
    )
)

stats = (
    windowed
    .groupBy("claim_date_decile")
    .agg(
        F.round(100 * F.count(F.when(~F.col("status").isin("IN_REVIEW", "OPEN"), 1)) / F.count("*"), 1).alias("pct_status_not_in_in_review_open"),
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("n_status_open"),
        F.count("*").alias("cnt"),
        F.avg("reported_at_percent_rank").alias("avg_reported_at_percent_rank_check"),
    )
    .filter(F.col("avg_reported_at_percent_rank_check") > 0.1)
    .drop("avg_reported_at_percent_rank_check")
)

result = (
    stats
    .select(
        "claim_date_decile",
        "n_status_open",
        "cnt",
        F.abs(F.col("claim_date_decile") - 4).alias("claim_date_decile_distance"),
        F.coalesce(F.col("n_status_open"), F.lit(0)).alias("n_status_open_filled"),
    )
    .orderBy(F.col("cnt").desc(), F.col("n_status_open").asc(), "claim_date_decile")
    .limit(10)
)
