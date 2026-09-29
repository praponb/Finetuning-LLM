from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id = Window.partitionBy("policy_id")
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_approved_amou_2 = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id")
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_all_by_approved_amount_runni = Window.orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_claim_date_year = Window.partitionBy(F.year("second_claim_date"))
w_adjuster_id_approved_amount = Window.partitionBy("adjuster_id", "approved_amount_seq").orderBy(F.col("policy_id_avg_claimed_amount").desc())
w_policy_id_by_claimed_amount_2 = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount").asc_nulls_last(), "id")
w_all_by_second_claim_date_run = Window.orderBy(F.col("second_claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_claimed_amount_3 = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount").asc_nulls_last())
w_approved_amount_seq_adjuster = Window.partitionBy("approved_amount_seq", "adjuster_id")

cleaned_claims = (
    df_claims
    .filter(F.col("claimed_amount") <= 41878.5)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
    )
)

windowed = (
    cleaned_claims
    .select(
        "*",
        F.nth_value("claim_date", 2).over(w_policy_id_by_claimed_amount).alias("second_claim_date"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.last("approved_amount", ignorenulls=True).over(w_adjuster_id_by_approved_amou).alias("last_known_approved_amount"),
        F.max("claimed_amount").over(w_policy_id).alias("policy_id_max_claimed_amount"),
        F.row_number().over(w_adjuster_id_by_approved_amou_2).alias("approved_amount_seq"),
        F.rank().over(w_all_by_approved_amount).alias("approved_amount_rank"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id"),
        F.min("approved_amount").over(w_policy_id).alias("policy_id_min_approved_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id), 2).alias("approved_amount_vs_policy_id_avg"),
        F.round(F.avg("claimed_amount").over(w_policy_id), 2).alias("policy_id_avg_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id), 2).alias("claimed_amount_vs_policy_id_avg"),
        F.first("claimed_amount", ignorenulls=True).over(w_all_by_approved_amount_runni).alias("first_known_claimed_amount"),
    )
)

top_rows = windowed.filter(F.col("approved_amount_rank") <= 5)

with_trends = (
    top_rows
    .select(
        "*",
        F.max("claimed_amount").over(w_second_claim_date_year).alias("second_claim_date_year_max_claimed_amount"),
        F.round(F.percent_rank().over(w_adjuster_id_approved_amount), 3).alias("policy_id_avg_claimed_amount_percent_rank"),
        F.row_number().over(w_policy_id_by_claimed_amount_2).alias("seq_no"),
        F.min("policy_id_avg_claimed_amount").over(w_all_by_second_claim_date_run).alias("min_policy_id_avg_claimed_amount_so_far"),
        F.round(F.cume_dist().over(w_policy_id_by_claimed_amount_3), 3).alias("claimed_amount_cume_dist"),
        F.round(F.col("last_known_approved_amount") - F.avg("last_known_approved_amount").over(w_approved_amount_seq_adjuster), 2).alias("last_known_approved_amount_vs_approved_amount_se"),
    )
)

result = (
    with_trends
    .filter(F.col("second_claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "rows_in_policy_id",
        "last_known_approved_amount",
        "policy_id_max_claimed_amount",
        "approved_amount_seq",
        "approved_amount_rank",
        "claimed_amount_share_of_policy_id",
        "policy_id_min_approved_amount",
        "approved_amount_vs_policy_id_avg",
        "policy_id_avg_claimed_amount",
        "first_known_claimed_amount",
        "second_claim_date_year_max_claimed_amount",
        "seq_no",
        "min_policy_id_avg_claimed_amount_so_far",
        "claimed_amount_cume_dist",
        "last_known_approved_amount_vs_approved_amount_se",
        F.date_format(F.col("second_claim_date"), "yyyy-MM").alias("second_claim_date_label"),
    )
    .orderBy(
        F.col("claimed_amount").desc(),
        F.col("last_known_approved_amount_vs_approved_amount_se").desc(),
    )
)
