from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount")
w_all_by_claim_date_ties = Window.orderBy("claim_date")
w_claim_date_month_policy_id_b = Window.partitionBy(F.month("claim_date"), "policy_id").orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")
w_all_by_approved_amount_movin = Window.orderBy("approved_amount", "id").rowsBetween(-2, Window.currentRow)
w_claim_date_month_by_claimed = Window.partitionBy(F.month("claim_date")).orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_by_claimed_2 = Window.partitionBy(F.month("claim_date")).orderBy(F.col("claimed_amount").asc_nulls_last(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_policy_id_claim_date_month = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date"))
w_adjuster_id_by_claim_date_fu = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount_movin_2 = Window.orderBy("approved_amount", "id").rowsBetween(-3, Window.currentRow)
w_claim_date_month_policy_id_b_2 = Window.partitionBy(F.month("claim_date"), "policy_id").orderBy(F.col("approved_amount").desc_nulls_first())
w_all_by_claim_date = Window.orderBy("claim_date", "id")

valid_claims = (
    df_claims
    .filter((F.col("claim_type") == "collision") & (F.col("status") == "OPEN"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
    )
)

ranked = (
    valid_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        F.round(F.cume_dist().over(w_policy_id_by_approved_amount), 3).alias("approved_amount_cume_dist"),
        F.rank().over(w_all_by_claim_date_ties).alias("claim_date_rank"),
        F.lag("claimed_amount").over(w_claim_date_month_policy_id_b).alias("prev_claimed_amount"),
        F.round(F.avg("claimed_amount").over(w_policy_id_adjuster_id), 2).alias("policy_id_adjuster_id_avg_claimed_amount"),
        F.max("claimed_amount").over(w_all_by_approved_amount_movin).alias("claimed_amount_max_3"),
        F.last("approved_amount", ignorenulls=True).over(w_claim_date_month_by_claimed).alias("last_known_approved_amount"),
        F.lead("claimed_amount").over(w_claim_date_month_by_claimed_2).alias("next_claimed_amount"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.round(F.avg("approved_amount").over(w_policy_id_claim_date_month), 2).alias("policy_id_claim_date_month_avg_approved_amount"),
        F.lag("claim_date").over(w_claim_date_month_by_claimed_2).alias("prev_claim_date"),
        F.nth_value("approved_amount", 2).over(w_adjuster_id_by_claim_date_fu).alias("second_approved_amount"),
        F.round(F.avg("claimed_amount").over(w_all_by_approved_amount_movin_2), 2).alias("claimed_amount_avg_4"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id_claim_date_month), 2).alias("claimed_amount_vs_policy_id_claim_date_month_avg"),
        F.round(F.cume_dist().over(w_claim_date_month_policy_id_b_2), 3).alias("approved_amount_cume_dist_2"),
        F.dense_rank().over(w_all_by_claim_date).alias("claim_date_dense_rank"),
    )
)

annotated = (
    ranked
    .withColumn("claimed_amount_max_3_tier", F.when(F.col("claimed_amount_max_3") >= 97535.0, "large").when(F.col("claimed_amount_max_3") >= 37575.0, "medium").otherwise("small"))
)

result = (
    annotated
    .filter(F.col("adjuster_id") >= 3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "approved_amount_cume_dist",
        "claim_date_rank",
        "prev_claimed_amount",
        "policy_id_adjuster_id_avg_claimed_amount",
        "claimed_amount_max_3",
        "last_known_approved_amount",
        "prev_claim_date",
        "second_approved_amount",
        "claimed_amount_avg_4",
        "claimed_amount_vs_policy_id_claim_date_month_avg",
        "approved_amount_cume_dist_2",
        "claimed_amount_max_3_tier",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
    .orderBy("prev_claim_date")
)
