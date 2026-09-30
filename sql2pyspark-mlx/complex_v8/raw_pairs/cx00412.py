from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_greatest_claim = Window.partitionBy("claim_type").orderBy(F.col("greatest_claimed_amount_approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type = Window.partitionBy("claim_type")
w_all_by_greatest_claimed_amou = Window.orderBy("greatest_claimed_amount_approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_status = Window.partitionBy(F.date_trunc("month", "claim_date"), "status")
w_adjuster_team_by_claim_date = Window.partitionBy("adjuster_team").orderBy(F.col("claim_date").desc_nulls_first(), "id")
w_adjuster_team_by_claim_date_2 = Window.partitionBy("adjuster_team").orderBy(F.col("claim_date").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_claim_type_by_greatest_claim_2 = Window.partitionBy("claim_type").orderBy(F.col("greatest_claimed_amount_approved_amount").desc(), "id")
w_all_by_greatest_claimed_amou_2 = Window.orderBy("greatest_claimed_amount_approved_amount", "id")
w_adjuster_team_by_claim_date_3 = Window.partitionBy("adjuster_team").orderBy(F.col("claim_date").desc_nulls_first())
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-6, Window.currentRow)
w_claim_date_dense_rank_status = Window.partitionBy("claim_date_dense_rank", "status_len_2")

recent_claims = (
    df_claims
    .filter(F.col("claim_type").isin("medical", "theft"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

combined_details = (
    recent_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select("s.*", F.col("a.team").alias("adjuster_team"))
)

with_windows = (
    combined_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "greatest_claimed_amount_approved_amount",
        "status_len",
        F.nth_value("approved_amount", 2).over(w_claim_type_by_greatest_claim).alias("second_approved_amount"),
        F.round(F.avg("status_len").over(w_claim_type), 2).alias("claim_type_avg_status_len"),
        F.nth_value("status_len", 2).over(w_all_by_greatest_claimed_amou).alias("second_status_len"),
        F.max("approved_amount").over(w_claim_type).alias("claim_type_max_approved_amount"),
        F.round(F.col("status_len") - F.avg("status_len").over(w_claim_date_month_status), 2).alias("status_len_vs_claim_date_month_status_avg"),
        F.lag("approved_amount", 2).over(w_adjuster_team_by_claim_date).alias("prev2_approved_amount"),
        F.size(F.collect_set("adjuster_id").over(w_claim_date_month_status)).alias("distinct_adjuster_id_in_claim_date_month_status"),
        F.lead("status_len").over(w_adjuster_team_by_claim_date).alias("next_status_len"),
        F.sum("approved_amount").over(w_adjuster_team_by_claim_date_2).alias("approved_amount_sum_6_prior"),
        F.size(F.collect_set("adjuster_id").over(w_claim_type)).alias("distinct_adjuster_id_in_claim_type"),
        F.count(F.lit(1)).over(w_claim_date_month_status).alias("rows_in_claim_date_month_status"),
        F.ntile(3).over(w_claim_type_by_greatest_claim_2).alias("greatest_claimed_amount_approved_amount_tercile"),
        F.lead("status_len").over(w_all_by_greatest_claimed_amou_2).alias("next_status_len_2"),
        F.dense_rank().over(w_adjuster_team_by_claim_date_3).alias("claim_date_dense_rank"),
        F.round(F.avg("status_len").over(w_adjuster_id_by_approved_amou), 2).alias("status_len_avg_7"),
    )
)

tagged = with_windows.withColumn("status_len_2", F.length(F.trim(F.col("status"))))

windowed = (
    tagged
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_date_dense_rank_status).alias("rows_in_claim_date_dense_rank_status_len_2"),
        F.max("claim_type_avg_status_len").over(w_claim_date_dense_rank_status).alias("claim_date_dense_rank_status_len_2_max_claim_typ"),
        F.min("next_status_len").over(w_claim_date_dense_rank_status).alias("claim_date_dense_rank_status_len_2_min_next_stat"),
        F.round(F.col("second_approved_amount") / F.nullif(F.sum("second_approved_amount").over(w_claim_date_dense_rank_status), F.lit(0)), 4).alias("second_approved_amount_share_of_claim_date_dense"),
    )
)

result = (
    windowed
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "greatest_claimed_amount_approved_amount",
        "claim_type_avg_status_len",
        "second_status_len",
        "status_len_vs_claim_date_month_status_avg",
        "prev2_approved_amount",
        "next_status_len",
        "approved_amount_sum_6_prior",
        "distinct_adjuster_id_in_claim_type",
        "rows_in_claim_date_month_status",
        "greatest_claimed_amount_approved_amount_tercile",
        "next_status_len_2",
        "claim_date_dense_rank",
        "status_len_avg_7",
        "status_len_2",
        "claim_date_dense_rank_status_len_2_max_claim_typ",
        "claim_date_dense_rank_status_len_2_min_next_stat",
        "second_approved_amount_share_of_claim_date_dense",
    )
    .orderBy("second_status_len", "id")
    .limit(5)
)
