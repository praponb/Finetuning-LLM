from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_adjuster_id_by_cla = Window.partitionBy("policy_id", "adjuster_id").orderBy("claim_date_dayofweek", "id")
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").asc_nulls_last())
w_all_by_claim_date = Window.orderBy(F.col("claim_date").asc_nulls_last(), "id")
w_claim_date_dayofweek_policy = Window.partitionBy("claim_date_dayofweek", "policy_id")
w_claim_type = Window.partitionBy("claim_type")
w_all_by_claim_date_full = Window.orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_claim_date_month = Window.partitionBy("adjuster_id", F.date_trunc("month", "claim_date_month")).orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_claim_date_month_2 = Window.partitionBy("adjuster_id", F.date_trunc("month", "claim_date_month")).orderBy(F.col("claim_date").desc(), "id")
w_claim_date_year_by_claim_dat = Window.partitionBy(F.year("claim_date")).orderBy("claim_date_month", "id")
w_all_by_days_since_claim_date = Window.orderBy("days_since_claim_date", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_adjuster_id_claim_date_month_3 = Window.partitionBy("adjuster_id", F.date_trunc("month", "claim_date_month")).orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_claim_date_pe = Window.partitionBy("adjuster_id").orderBy("claim_date_percent_rank", "id")
w_all_by_days_since_claim_date_2 = Window.orderBy("days_since_claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "claim_date_dayofweek",
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_policy_id_adjuster_id_by_cla))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.round(F.cume_dist().over(w_all_by_reported_at_ties), 3).alias("reported_at_cume_dist"),
        F.round(F.percent_rank().over(w_policy_id_adjuster_id_by_cla), 3).alias("claim_date_dayofweek_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_claim_date), 3).alias("claim_date_percent_rank"),
        F.count(F.lit(1)).over(w_claim_date_dayofweek_policy).alias("rows_in_claim_date_dayofweek_policy_id"),
        F.size(F.collect_set("policy_id").over(w_claim_type)).alias("distinct_policy_id_in_claim_type"),
        F.sum("claim_date_dayofweek").over(w_claim_date_dayofweek_policy).alias("claim_date_dayofweek_policy_id_sum_claim_date_da"),
        F.last("claimed_amount").over(w_all_by_claim_date_full).alias("last_claimed_amount"),
        F.min("claim_date_dayofweek").over(w_claim_date_dayofweek_policy).alias("claim_date_dayofweek_policy_id_min_claim_date_da"),
    )
)

annotated = (
    metrics
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "reported_at_cume_dist",
        "claim_date_dayofweek_percent_rank",
        "claim_date_cume_dist",
        "claim_date_percent_rank",
        "claim_date_dayofweek_policy_id_sum_claim_date_da",
        "claim_date_dayofweek_policy_id_min_claim_date_da",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
    )
)

with_windows = (
    annotated
    .select(
        "*",
        F.min("claim_date_cume_dist").over(w_adjuster_id_claim_date_month).alias("min_claim_date_cume_dist_so_far"),
        F.dense_rank().over(w_adjuster_id_claim_date_month_2).alias("claim_date_dense_rank"),
        F.row_number().over(w_claim_date_year_by_claim_dat).alias("seq_no"),
        F.lead("days_since_claim_date", 2).over(w_all_by_days_since_claim_date).alias("next2_days_since_claim_date"),
        F.round(F.col("claim_date_percent_rank") - F.avg("claim_date_percent_rank").over(w_adjuster_id), 2).alias("claim_date_percent_rank_vs_adjuster_id_avg"),
        F.nth_value("days_since_claim_date", 2).over(w_adjuster_id_claim_date_month_3).alias("second_days_since_claim_date"),
        F.lag("days_since_claim_date").over(w_adjuster_id_by_claim_date_pe).alias("prev_days_since_claim_date"),
        F.first("claim_date_cume_dist", ignorenulls=True).over(w_all_by_days_since_claim_date_2).alias("first_known_claim_date_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "claim_date_dayofweek_percent_rank",
        "claim_date_dayofweek_policy_id_min_claim_date_da",
        "claim_date_month",
        "claim_date_dense_rank",
        "seq_no",
        "next2_days_since_claim_date",
        "prev_days_since_claim_date",
        "first_known_claim_date_cume_dist",
        F.when(F.col("claim_date_percent_rank") >= 0.8, "large").when(F.col("claim_date_percent_rank") >= 0.3, "medium").otherwise("small").alias("claim_date_percent_rank_tier"),
    )
    .orderBy(F.col("claim_date_dayofweek_policy_id_min_claim_date_da").asc())
)
