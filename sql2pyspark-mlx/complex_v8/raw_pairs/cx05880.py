from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_greatest_claimed_amou = Window.orderBy(F.col("greatest_claimed_amount_approved_amount").desc(), "id")
w_status_group_policy_id_by_ap = Window.partitionBy("status_group", "policy_id").orderBy("approved_amount", "id")
w_all_by_greatest_claimed_amou_2 = Window.orderBy(F.col("greatest_claimed_amount_approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_group_claim_type = Window.partitionBy("status_group", "claim_type")
w_status_group_policy_id_by_gr = Window.partitionBy("status_group", "policy_id").orderBy(F.col("greatest_claimed_amount_approved_amount").asc_nulls_last(), "id")
w_status_group_policy_id_by_gr_2 = Window.partitionBy("status_group", "policy_id").orderBy(F.col("greatest_claimed_amount_approved_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_policy_id_greatest_claimed_a = Window.partitionBy("policy_id", "greatest_claimed_amount_approved_amount_quartile").orderBy(F.col("approved_amount_seq").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_approved_amount_seq_by_great = Window.partitionBy("approved_amount_seq").orderBy("greatest_claimed_amount_approved_amount_quartile", "id")

base_claims = (
    df_claims
    .filter((F.col("policy_id") <= 26) & (F.col("status") == "IN_REVIEW"))
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
        F.when(F.col("status").isin("OPEN", "PAID"), "group_a").when(F.col("status") == "IN_REVIEW", "group_b").otherwise("other").alias("status_group"),
    )
)

with_windows = (
    base_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "greatest_claimed_amount_approved_amount",
        F.round(F.cume_dist().over(w_all_by_greatest_claimed_amou), 3).alias("greatest_claimed_amount_approved_amount_cume_dis"),
        F.lag("claimed_amount", 3).over(w_status_group_policy_id_by_ap).alias("prev3_claimed_amount"),
        F.count(F.lit(1)).over(w_all_by_greatest_claimed_amou_2).alias("running_count"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status_group_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_status_group_claim_type"),
        F.row_number().over(w_status_group_policy_id_by_ap).alias("approved_amount_seq"),
        F.max("approved_amount").over(w_status_group_claim_type).alias("status_group_claim_type_max_approved_amount"),
        F.count(F.lit(1)).over(w_status_group_claim_type).alias("rows_in_status_group_claim_type"),
        F.lag("claimed_amount").over(w_status_group_policy_id_by_ap).alias("prev_claimed_amount"),
        F.max("claimed_amount").over(w_status_group_claim_type).alias("status_group_claim_type_max_claimed_amount"),
        F.row_number().over(w_status_group_policy_id_by_gr).alias("greatest_claimed_amount_approved_amount_seq"),
        F.ntile(4).over(w_status_group_policy_id_by_gr).alias("greatest_claimed_amount_approved_amount_quartile"),
        F.dense_rank().over(w_all_by_greatest_claimed_amou).alias("greatest_claimed_amount_approved_amount_dense_ra"),
        F.nth_value("approved_amount", 2).over(w_status_group_policy_id_by_gr_2).alias("second_approved_amount"),
        F.sum("approved_amount").over(w_status_group_claim_type).alias("status_group_claim_type_sum_approved_amount"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.lead("claim_date", 2).over(w_all_by_claimed_amount).alias("next2_claim_date"),
        F.first("second_approved_amount", ignorenulls=True).over(w_policy_id_greatest_claimed_a).alias("first_known_second_approved_amount"),
        F.row_number().over(w_approved_amount_seq_by_great).alias("rn"),
        F.lag("prev3_claimed_amount").over(w_all_by_claimed_amount).alias("prev_prev3_claimed_amount"),
    )
)

result = (
    windowed
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "status",
        "greatest_claimed_amount_approved_amount",
        "greatest_claimed_amount_approved_amount_cume_dis",
        "prev3_claimed_amount",
        "running_count",
        "approved_amount_share_of_status_group_claim_type",
        "approved_amount_seq",
        "status_group_claim_type_max_approved_amount",
        "prev_claimed_amount",
        "status_group_claim_type_max_claimed_amount",
        "greatest_claimed_amount_approved_amount_seq",
        "greatest_claimed_amount_approved_amount_quartile",
        "second_approved_amount",
        "status_group_claim_type_sum_approved_amount",
        "next2_claim_date",
        "first_known_second_approved_amount",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.add_months(F.col("next2_claim_date"), 6).alias("next2_claim_date_plus_6m"),
    )
)
