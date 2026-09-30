from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_status_adjuster_id_by_approv = Window.partitionBy("status", "adjuster_id").orderBy(F.col("approved_amount").desc())
w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount")
w_reported_at_year_by_claimed = Window.partitionBy(F.year("reported_at")).orderBy(F.col("claimed_amount").desc(), "id")
w_policy_id_by_claimed_amount_2 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id")
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_all_by_approved_amount_runni = Window.orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_approved_amount_movin = Window.orderBy("approved_amount", "id").rowsBetween(-2, Window.currentRow)
w_status_policy_id_by_approved = Window.partitionBy("status", "policy_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_claimed_amount_3 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(-3, -1)
w_reported_at_to_date_policy_i = Window.partitionBy(F.to_date("reported_at"), "policy_id").orderBy("approved_amount", "id")
w_status_policy_id_by_approved_2 = Window.partitionBy("status", "policy_id").orderBy("approved_amount", "id")
w_reported_at_year_by_claimed_2 = Window.partitionBy(F.year("reported_at")).orderBy(F.col("claimed_amount").desc())
w_adjuster_id_approved_amount = Window.partitionBy("adjuster_id", "approved_amount_rank")
w_status_adjuster_id_by_claime = Window.partitionBy("status", "adjuster_id").orderBy(F.col("claimed_amount_rank").asc_nulls_last(), "id")
w_all_by_last_known_claimed_am = Window.orderBy(F.col("last_known_claimed_amount").desc(), "id")
w_all_by_claimed_amount_avg_3 = Window.orderBy("claimed_amount_avg_3")

recent_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

sequenced = (
    recent_claims
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "claim_type_prefix",
        F.round(F.avg("claimed_amount").over(w_status), 2).alias("status_avg_claimed_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_status_adjuster_id"),
        F.round(F.cume_dist().over(w_status_adjuster_id_by_approv), 3).alias("approved_amount_cume_dist"),
        F.round(F.avg("claimed_amount").over(w_status_adjuster_id), 2).alias("status_adjuster_id_avg_claimed_amount"),
        F.rank().over(w_policy_id_by_claimed_amount).alias("claimed_amount_rank"),
        F.lead("approved_amount").over(w_reported_at_year_by_claimed).alias("next_approved_amount"),
        F.size(F.collect_set("policy_id").over(w_status_adjuster_id)).alias("distinct_policy_id_in_status_adjuster_id"),
        F.rank().over(w_policy_id_by_claimed_amount_2).alias("claimed_amount_rank_2"),
        F.rank().over(w_all_by_approved_amount).alias("approved_amount_rank"),
        F.last("claimed_amount", ignorenulls=True).over(w_all_by_approved_amount_runni).alias("last_known_claimed_amount"),
        F.round(F.avg("claimed_amount").over(w_all_by_approved_amount_movin), 2).alias("claimed_amount_avg_3"),
        F.first("approved_amount", ignorenulls=True).over(w_status_policy_id_by_approved).alias("first_known_approved_amount"),
        F.max("approved_amount").over(w_policy_id_by_claimed_amount_3).alias("approved_amount_max_3_prior"),
        F.lag("approved_amount").over(w_reported_at_to_date_policy_i).alias("prev_approved_amount"),
        F.round(F.percent_rank().over(w_status_policy_id_by_approved_2), 3).alias("approved_amount_percent_rank"),
        F.round(F.percent_rank().over(w_reported_at_year_by_claimed_2), 3).alias("claimed_amount_percent_rank"),
    )
)

ranked = (
    sequenced
    .select(
        "*",
        F.count(F.lit(1)).over(w_adjuster_id_approved_amount).alias("rows_in_adjuster_id_approved_amount_rank"),
        F.size(F.collect_set("status").over(w_adjuster_id_approved_amount)).alias("distinct_status_in_adjuster_id_approved_amount_r"),
        F.row_number().over(w_status_adjuster_id_by_claime).alias("claimed_amount_rank_seq"),
        F.round(F.percent_rank().over(w_all_by_last_known_claimed_am), 3).alias("last_known_claimed_amount_percent_rank"),
        F.dense_rank().over(w_all_by_claimed_amount_avg_3).alias("claimed_amount_avg_3_dense_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        "claim_type_prefix",
        "claimed_amount_rank",
        "next_approved_amount",
        "distinct_policy_id_in_status_adjuster_id",
        "claimed_amount_rank_2",
        "approved_amount_rank",
        "last_known_claimed_amount",
        "claimed_amount_avg_3",
        "approved_amount_max_3_prior",
        "approved_amount_percent_rank",
        "rows_in_adjuster_id_approved_amount_rank",
        "distinct_status_in_adjuster_id_approved_amount_r",
        "last_known_claimed_amount_percent_rank",
        "claimed_amount_avg_3_dense_rank",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
    .orderBy(F.col("claimed_amount").desc(), F.col("id").desc())
)
