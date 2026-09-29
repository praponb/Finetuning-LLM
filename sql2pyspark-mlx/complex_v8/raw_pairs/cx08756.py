from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_date_month_b = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date")).orderBy(F.col("claimed_amount").desc(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_all_by_claimed_amount_minus = Window.orderBy(F.col("claimed_amount_minus_approved_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_claim_date_month_b_2 = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date")).orderBy(F.col("claimed_amount").desc())
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_all_by_claimed_amount_minus_2 = Window.orderBy(F.col("claimed_amount_minus_approved_amount").asc_nulls_last(), "id")
w_all_by_claimed_amount_minus_3 = Window.orderBy(F.col("claimed_amount_minus_approved_amount").asc_nulls_last())

cleaned_claims = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")) | (F.col("status") == "REJECTED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

joined_adjuster = (
    cleaned_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.team").isin("Property", "Motor"))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.reported_at",
        "s.claimed_amount_minus_approved_amount",
        F.col("a.team").alias("adjuster_team"),
    )
)

ranked = (
    joined_adjuster
    .select(
        "*",
        F.lead("reported_at").over(w_policy_id_claim_date_month_b).alias("next_reported_at"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id), 2).alias("claimed_amount_vs_policy_id_avg"),
        F.min("claimed_amount").over(w_policy_id).alias("policy_id_min_claimed_amount"),
        F.first("reported_at").over(w_all_by_claimed_amount_minus).alias("first_reported_at"),
        F.lag("reported_at").over(w_policy_id_claim_date_month_b).alias("prev_reported_at"),
        F.round(F.percent_rank().over(w_policy_id_claim_date_month_b_2), 3).alias("claimed_amount_percent_rank"),
        F.round(F.cume_dist().over(w_policy_id_claim_date_month_b), 3).alias("claimed_amount_cume_dist"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id"),
        F.row_number().over(w_all_by_reported_at).alias("rn"),
        F.lag("claim_date", 2).over(w_all_by_reported_at).alias("prev2_claim_date"),
        F.nth_value("reported_at", 2).over(w_all_by_claimed_amount_minus).alias("second_reported_at"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount_minus_2), 3).alias("claimed_amount_minus_approved_amount_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount_minus_3), 3).alias("claimed_amount_minus_approved_amount_percent_ran"),
    )
)

result = (
    ranked
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount_minus_approved_amount",
        "adjuster_team",
        "claimed_amount_vs_policy_id_avg",
        "policy_id_min_claimed_amount",
        "prev_reported_at",
        "claimed_amount_percent_rank",
        "claimed_amount_share_of_policy_id",
        "prev2_claim_date",
        "second_reported_at",
        "claimed_amount_minus_approved_amount_cume_dist",
        "claimed_amount_minus_approved_amount_percent_ran",
        F.least(F.col("rn"), F.col("claimed_amount")).alias("least_rn_claimed_amount"),
    )
    .orderBy(
        F.col("adjuster_id").asc(),
        F.col("claimed_amount_minus_approved_amount_cume_dist").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
