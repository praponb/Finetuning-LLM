from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount")
w_status = Window.partitionBy("status")
w_status_by_reported_at_ties = Window.partitionBy("status").orderBy("reported_at")
w_policy_id_by_claimed_amount_2 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id")
w_reported_at_hour_by_claimed = Window.partitionBy("reported_at_hour").orderBy("claimed_amount", "id")
w_reported_at_hour_by_claimed_2 = Window.partitionBy("reported_at_hour").orderBy("claimed_amount", "id").rowsBetween(-3, Window.currentRow)
w_policy_id_by_claimed_amount_3 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claimed_amount_seq_policy_id = Window.partitionBy("claimed_amount_seq", "policy_id").orderBy(F.col("reported_at").desc(), "id")
w_claimed_amount_seq_policy_id_2 = Window.partitionBy("claimed_amount_seq", "policy_id").orderBy(F.col("reported_at").desc())
w_reported_at_to_date = Window.partitionBy(F.to_date("reported_at"))
w_all_by_claimed_amount_seq_ru = Window.orderBy(F.col("claimed_amount_seq").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claimed_amount_seq_ti = Window.orderBy(F.col("claimed_amount_seq").desc())
w_all_by_claimed_amount_seq_st = Window.orderBy("claimed_amount_seq_status_rejected")
w_all_by_claimed_amount_seq_st_2 = Window.orderBy("claimed_amount_seq_status_rejected", "status", "distinct_claim_type_in_reported_at_to_date")

scoped = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("policy_id") <= 10))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.hour(F.col("reported_at")).alias("reported_at_hour"),
    )
)

with_trends = (
    scoped
    .select(
        "*",
        F.rank().over(w_policy_id_by_claimed_amount).alias("claimed_amount_rank"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.round(F.percent_rank().over(w_status_by_reported_at_ties), 3).alias("reported_at_percent_rank"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status), F.lit(0)), 4).alias("claimed_amount_share_of_status"),
        F.row_number().over(w_policy_id_by_claimed_amount_2).alias("claimed_amount_seq"),
        F.row_number().over(w_reported_at_hour_by_claimed).alias("seq_no"),
        F.round(F.avg("claimed_amount").over(w_reported_at_hour_by_claimed_2), 2).alias("claimed_amount_avg_4"),
        F.last("claim_date", ignorenulls=True).over(w_policy_id_by_claimed_amount_3).alias("last_known_claim_date"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "status",
        "claimed_amount_rank",
        "reported_at_percent_rank",
        "claimed_amount_share_of_status",
        "claimed_amount_seq",
        (F.col("reported_at_hour") - F.lag("reported_at_hour").over(w_claimed_amount_seq_policy_id)).alias("reported_at_hour_change"),
        F.rank().over(w_claimed_amount_seq_policy_id_2).alias("reported_at_rank"),
        F.count(F.lit(1)).over(w_reported_at_to_date).alias("rows_in_reported_at_to_date"),
        F.size(F.collect_set("claim_type").over(w_reported_at_to_date)).alias("distinct_claim_type_in_reported_at_to_date"),
        F.min("claimed_amount_avg_4").over(w_reported_at_to_date).alias("reported_at_to_date_min_claimed_amount_avg_4"),
        F.last("claim_date", ignorenulls=True).over(w_all_by_claimed_amount_seq_ru).alias("last_known_claim_date_2"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount_seq_ti), 3).alias("claimed_amount_seq_cume_dist"),
        F.round(F.cume_dist().over(w_claimed_amount_seq_policy_id_2), 3).alias("reported_at_cume_dist"),
    )
)

totals = (
    with_windows
    .filter(F.year(F.col("last_known_claim_date_2")) == 2025)
    .groupBy("status", "distinct_claim_type_in_reported_at_to_date")
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("status") == "REJECTED", F.col("claimed_amount_seq"))).alias("claimed_amount_seq_status_rejected"),
        F.avg("claimed_amount_seq_cume_dist").alias("avg_claimed_amount_seq_cume_dist_check"),
    )
    .filter(F.col("avg_claimed_amount_seq_cume_dist_check") > 0.4)
    .drop("avg_claimed_amount_seq_cume_dist_check")
)

with_trends_2 = (
    totals
    .select(
        "*",
        F.rank().over(w_all_by_claimed_amount_seq_st).alias("claimed_amount_seq_status_rejected_rank"),
        F.ntile(4).over(w_all_by_claimed_amount_seq_st_2).alias("claimed_amount_seq_status_rejected_quartile"),
        F.row_number().over(w_all_by_claimed_amount_seq_st_2).alias("claimed_amount_seq_status_rejected_seq"),
    )
)

result = (
    with_trends_2
    .filter(F.col("distinct_claim_type_in_reported_at_to_date") > 5)
    .select(
        "status",
        "distinct_claim_type_in_reported_at_to_date",
        "status_list",
        "claimed_amount_seq_status_rejected",
        "claimed_amount_seq_status_rejected_rank",
        "claimed_amount_seq_status_rejected_seq",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
    .orderBy("claimed_amount_seq_status_rejected_seq")
)
