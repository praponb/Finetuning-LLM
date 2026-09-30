from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy("reported_at", "id")
w_claim_type = Window.partitionBy("claim_type")
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_policy_id = Window.partitionBy("policy_id")
w_all_by_claim_date_ties = Window.orderBy("claim_date")
w_all_by_reported_at_running = Window.orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date_prior_6 = Window.orderBy("claim_date", "id").rowsBetween(-6, -1)
w_policy_id_by_status_len = Window.partitionBy("policy_id").orderBy("status_len", "id")
w_distinct_adjuster_id_in_clai = Window.partitionBy("distinct_adjuster_id_in_claim_type", "status_len").orderBy(F.col("status_len_share_of_claim_type").desc())
w_distinct_adjuster_id_in_clai_2 = Window.partitionBy("distinct_adjuster_id_in_claim_type", "status_len").orderBy(F.col("status_len_share_of_claim_type").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_adjuster_id_in_clai_3 = Window.partitionBy("distinct_adjuster_id_in_claim_type", "status_len").orderBy(F.col("status_len_share_of_claim_type").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_distinct_ad = Window.partitionBy(F.month("claim_date"), "distinct_adjuster_id_in_claim_type")

scoped_claims = (
    df_claims
    .filter((F.col("status") == "REJECTED") | (F.col("claim_type") != "delay"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

metrics = (
    scoped_claims
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        "status_len",
        F.ntile(4).over(w_policy_id_by_reported_at).alias("reported_at_quartile"),
        F.round(F.col("status_len") / F.nullif(F.sum("status_len").over(w_claim_type), F.lit(0)), 4).alias("status_len_share_of_claim_type"),
        F.round(F.col("status_len") - F.avg("status_len").over(w_claim_type), 2).alias("status_len_vs_claim_type_avg"),
        F.ntile(10).over(w_all_by_claim_date).alias("claim_date_decile"),
        F.ntile(5).over(w_policy_id_by_reported_at).alias("reported_at_quintile"),
        F.row_number().over(w_policy_id_by_reported_at).alias("seq_no"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_all_by_claim_date))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.max("status_len").over(w_policy_id).alias("policy_id_max_status_len"),
        F.round(F.avg("status_len").over(w_claim_type), 2).alias("claim_type_avg_status_len"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id)).alias("distinct_adjuster_id_in_policy_id"),
        F.size(F.collect_set("adjuster_id").over(w_claim_type)).alias("distinct_adjuster_id_in_claim_type"),
        F.round(F.percent_rank().over(w_all_by_claim_date_ties), 3).alias("claim_date_percent_rank"),
        F.last("reported_at", ignorenulls=True).over(w_all_by_reported_at_running).alias("last_known_reported_at"),
        F.max("status_len").over(w_all_by_claim_date_prior_6).alias("status_len_max_6_prior"),
        F.row_number().over(w_policy_id_by_status_len).alias("seq_no_2"),
    )
)

top_per_group = metrics.filter(F.col("seq_no_2") <= 5)

with_windows = (
    top_per_group
    .select(
        "*",
        F.dense_rank().over(w_distinct_adjuster_id_in_clai).alias("status_len_share_of_claim_type_dense_rank"),
        F.nth_value("last_known_reported_at", 2).over(w_distinct_adjuster_id_in_clai_2).alias("second_last_known_reported_at"),
        F.first("claim_date", ignorenulls=True).over(w_distinct_adjuster_id_in_clai_3).alias("first_known_claim_date"),
        F.sum("seq_no").over(w_claim_date_month_distinct_ad).alias("claim_date_month_distinct_adjuster_id_in_claim_t"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "status_len",
        "reported_at_quartile",
        "status_len_share_of_claim_type",
        "status_len_vs_claim_type_avg",
        "reported_at_quintile",
        "seq_no",
        "minutes_since_prev_reported_at",
        "claim_type_avg_status_len",
        "distinct_adjuster_id_in_policy_id",
        "distinct_adjuster_id_in_claim_type",
        "status_len_max_6_prior",
        "status_len_share_of_claim_type_dense_rank",
        "second_last_known_reported_at",
        "first_known_claim_date",
        "claim_date_month_distinct_adjuster_id_in_claim_t",
    )
    .orderBy(
        F.col("reported_at_quintile").desc(),
        F.col("status_len_share_of_claim_type_dense_rank").desc(),
        "id",
    )
    .limit(10)
)
