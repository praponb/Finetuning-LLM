from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_policy_id_by_claimed = Window.partitionBy("status", "policy_id").orderBy("claimed_amount")
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_status_claim_type = Window.partitionBy("status", "claim_type")
w_claim_date_month_policy_id_b = Window.partitionBy(F.date_trunc("month", "claim_date"), "policy_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_status_policy_id_by_claimed_2 = Window.partitionBy("status", "policy_id").orderBy("claimed_amount", "id")
w_claim_date_month_policy_id_b_2 = Window.partitionBy(F.date_trunc("month", "claim_date"), "policy_id").orderBy("reported_at", "id")
w_all_by_status_adjuster_id_ma = Window.orderBy("status_adjuster_id_max_claimed_amount", "id").rowsBetween(-2, -1)
w_all_by_claim_date_2 = Window.orderBy("claim_date", "id")
w_all_by_status_adjuster_id_ma_2 = Window.orderBy("status_adjuster_id_max_claimed_amount", "id")
w_all_by_claim_date_full = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_policy_id_in_status = Window.partitionBy("distinct_policy_id_in_status_claim_type", "distinct_status_in_adjuster_id_policy_id")

scoped_claims = (
    df_claims
    .filter((F.col("approved_amount") < 16404.6) & (F.col("claim_type") == "delay"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

windowed = (
    scoped_claims
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        F.round(F.percent_rank().over(w_status_policy_id_by_claimed), 3).alias("claimed_amount_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_claim_date), 3).alias("claim_date_percent_rank"),
        F.size(F.collect_set("policy_id").over(w_status_claim_type)).alias("distinct_policy_id_in_status_claim_type"),
        F.last("claimed_amount").over(w_claim_date_month_policy_id_b).alias("last_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id_policy_id), 2).alias("claimed_amount_vs_adjuster_id_policy_id_avg"),
        F.size(F.collect_set("status").over(w_adjuster_id_policy_id)).alias("distinct_status_in_adjuster_id_policy_id"),
        F.count(F.lit(1)).over(w_status_adjuster_id).alias("rows_in_status_adjuster_id"),
        F.max("claimed_amount").over(w_status_adjuster_id).alias("status_adjuster_id_max_claimed_amount"),
        F.max("claimed_amount").over(w_adjuster_id_policy_id).alias("adjuster_id_policy_id_max_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status_claim_type), 2).alias("claimed_amount_vs_status_claim_type_avg"),
        F.lag("claim_date").over(w_status_policy_id_by_claimed_2).alias("prev_claim_date"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_status_adjuster_id"),
        F.row_number().over(w_claim_date_month_policy_id_b_2).alias("rn"),
    )
)

leaders = (
    windowed
    .filter(F.col("rn") <= 3)
    .drop("rn")
)

metrics = (
    leaders
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "claimed_amount_percent_rank",
        "claim_date_percent_rank",
        "distinct_policy_id_in_status_claim_type",
        "last_claimed_amount",
        "distinct_status_in_adjuster_id_policy_id",
        "rows_in_status_adjuster_id",
        "adjuster_id_policy_id_max_claimed_amount",
        "claimed_amount_vs_status_claim_type_avg",
        "prev_claim_date",
        F.round(F.avg("claim_date_percent_rank").over(w_all_by_status_adjuster_id_ma), 2).alias("claim_date_percent_rank_avg_2_prior"),
        F.round(F.percent_rank().over(w_all_by_claim_date_2), 3).alias("claim_date_percent_rank_2"),
        F.lead("claimed_amount").over(w_all_by_status_adjuster_id_ma_2).alias("next_claimed_amount"),
        F.nth_value("rows_in_status_adjuster_id", 2).over(w_all_by_claim_date_full).alias("second_rows_in_status_adjuster_id"),
        F.round(F.col("last_claimed_amount") / F.nullif(F.sum("last_claimed_amount").over(w_distinct_policy_id_in_status), F.lit(0)), 4).alias("last_claimed_amount_share_of_distinct_policy_id_"),
    )
)

result = (
    metrics
    .select(
        "id",
        "distinct_policy_id_in_status_claim_type",
        "last_claimed_amount",
        "distinct_status_in_adjuster_id_policy_id",
        "adjuster_id_policy_id_max_claimed_amount",
        "claimed_amount_vs_status_claim_type_avg",
        "prev_claim_date",
        "claim_date_percent_rank_2",
        "next_claimed_amount",
        "second_rows_in_status_adjuster_id",
        F.greatest(F.col("claimed_amount"), F.col("claim_date_percent_rank")).alias("greatest_claimed_amount_claim_date_percent_rank"),
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
)
