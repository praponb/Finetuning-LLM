from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_policy_prod = Window.partitionBy(F.month("claim_date"), "policy_product")
w_adjuster_id_by_policy_start = Window.partitionBy("adjuster_id").orderBy(F.col("policy_start_date").desc(), "id")
w_all_by_policy_start_date = Window.orderBy("policy_start_date", "id")
w_all_by_policy_start_date_pri = Window.orderBy("policy_start_date", "id").rowsBetween(-3, -1)
w_adjuster_id_by_policy_start_2 = Window.partitionBy("adjuster_id").orderBy(F.col("policy_start_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_policy_id = Window.partitionBy("policy_id")
w_all_by_policy_start_date_ful = Window.orderBy("policy_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_all_by_policy_start_date_mov = Window.orderBy("policy_start_date", "id").rowsBetween(-2, Window.currentRow)
w_policy_id_approved_amount_ma = Window.partitionBy("policy_id", "approved_amount_max_3_tier")
w_distinct_status_group_in_pol = Window.partitionBy("distinct_status_group_in_policy_id", "status_group").orderBy("greatest_approved_amount_seq_no", "id").rowsBetween(-6, -1)
w_prev_policy_start_date_month = Window.partitionBy(F.month("prev_policy_start_date"), "status").orderBy("policy_start_date_dense_rank", "id")
w_prev_policy_start_date_month_2 = Window.partitionBy(F.month("prev_policy_start_date"), "status").orderBy("policy_start_date_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_status_group_in_pol_2 = Window.partitionBy("distinct_status_group_in_policy_id", "status_group").orderBy("greatest_approved_amount_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_policy_start_date_mont = Window.partitionBy(F.date_trunc("month", "first_policy_start_date"), "seq_no")

scoped = (
    df_claims
    .filter((F.col("claim_type") == "theft") & (F.col("approved_amount") > 0.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.when(F.col("status").isin("IN_REVIEW", "OPEN"), "group_a").when(F.col("status") == "REJECTED", "group_b").otherwise("other").alias("status_group"),
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.product") == "HOME")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.status",
        "s.status_group",
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

with_windows = (
    with_parent
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_date_month_policy_prod).alias("rows_in_claim_date_month_policy_product"),
        F.lag("policy_start_date").over(w_adjuster_id_by_policy_start).alias("prev_policy_start_date"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_date_month_policy_prod), F.lit(0)), 4).alias("approved_amount_share_of_claim_date_month_policy"),
        F.dense_rank().over(w_all_by_policy_start_date).alias("policy_start_date_dense_rank"),
        F.round(F.avg("approved_amount").over(w_all_by_policy_start_date_pri), 2).alias("approved_amount_avg_3_prior"),
        F.round(F.avg("approved_amount").over(w_adjuster_id_by_policy_start_2), 2).alias("approved_amount_avg_3"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id)).alias("distinct_adjuster_id_in_policy_id"),
        F.first("policy_start_date").over(w_all_by_policy_start_date_ful).alias("first_policy_start_date"),
        F.sum("approved_amount").over(w_policy_id).alias("policy_id_sum_approved_amount"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_adjuster_id_by_policy_start)).alias("days_since_prev_claim_date"),
        F.size(F.collect_set("status_group").over(w_policy_id)).alias("distinct_status_group_in_policy_id"),
        F.row_number().over(w_all_by_policy_start_date).alias("seq_no"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_approved_amount)).alias("days_since_prev_claim_date_2"),
        F.max("approved_amount").over(w_all_by_policy_start_date_mov).alias("approved_amount_max_3"),
        F.max("approved_amount").over(w_policy_id).alias("policy_id_max_approved_amount"),
    )
)

labeled = (
    with_windows
    .select(
        "*",
        F.greatest(F.col("approved_amount"), F.col("seq_no")).alias("greatest_approved_amount_seq_no"),
        F.when(F.col("approved_amount_max_3") >= 127500.0, "large").when(F.col("approved_amount_max_3") >= 30000.0, "medium").otherwise("small").alias("approved_amount_max_3_tier"),
        F.least(F.col("seq_no"), F.col("approved_amount")).alias("least_seq_no_approved_amount"),
    )
)

sequenced = (
    labeled
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status_group",
        "policy_product",
        "rows_in_claim_date_month_policy_product",
        "prev_policy_start_date",
        "approved_amount_share_of_claim_date_month_policy",
        "approved_amount_avg_3",
        "first_policy_start_date",
        "policy_id_sum_approved_amount",
        "seq_no",
        "approved_amount_max_3",
        "policy_id_max_approved_amount",
        "greatest_approved_amount_seq_no",
        "approved_amount_max_3_tier",
        "least_seq_no_approved_amount",
        F.size(F.collect_set("policy_product").over(w_policy_id_approved_amount_ma)).alias("distinct_policy_product_in_policy_id_approved_am"),
        F.round(F.avg("approved_amount_avg_3").over(w_distinct_status_group_in_pol), 2).alias("approved_amount_avg_3_avg_6_prior"),
        F.rank().over(w_prev_policy_start_date_month).alias("policy_start_date_dense_rank_rank"),
        F.nth_value("days_since_prev_claim_date", 2).over(w_prev_policy_start_date_month_2).alias("second_days_since_prev_claim_date"),
        F.first("least_seq_no_approved_amount").over(w_distinct_status_group_in_pol_2).alias("first_least_seq_no_approved_amount"),
        F.count(F.lit(1)).over(w_first_policy_start_date_mont).alias("rows_in_first_policy_start_date_month_seq_no"),
    )
)

result = (
    sequenced
    .filter(F.col("second_days_since_prev_claim_date") >= 50)
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status_group",
        "rows_in_claim_date_month_policy_product",
        "prev_policy_start_date",
        "approved_amount_share_of_claim_date_month_policy",
        "approved_amount_avg_3",
        "first_policy_start_date",
        "policy_id_sum_approved_amount",
        "approved_amount_max_3",
        "policy_id_max_approved_amount",
        "greatest_approved_amount_seq_no",
        "approved_amount_max_3_tier",
        "approved_amount_avg_3_avg_6_prior",
        "second_days_since_prev_claim_date",
        "first_least_seq_no_approved_amount",
        "rows_in_first_policy_start_date_month_seq_no",
    )
)
