from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_policy_id_claim_type_by_appr = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_type_by_appr_2 = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("approved_amount").desc(), "id")
w_claim_date_month_reported_at = Window.partitionBy(F.month("claim_date"), F.month("reported_at")).orderBy("claim_date", "id")
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount_filled").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_claim_type = Window.partitionBy("claim_type")
w_adjuster_id_by_approved_amou_2 = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount_filled").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_claims
    .filter(F.month(F.col("reported_at")).isin(1, 2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.lag("claimed_amount", 1, 0).over(w_all_by_claim_date).alias("prev_claimed_amount"),
        F.size(F.collect_set("claim_type").over(w_adjuster_id_policy_id)).alias("distinct_claim_type_in_adjuster_id_policy_id"),
        F.row_number().over(w_all_by_claim_date).alias("rn"),
        F.first("approved_amount_filled", ignorenulls=True).over(w_policy_id_claim_type_by_appr).alias("first_known_approved_amount_filled"),
        F.lead("reported_at").over(w_policy_id_claim_type_by_appr_2).alias("next_reported_at"),
        F.ntile(10).over(w_all_by_claim_date).alias("claim_date_decile"),
        F.lead("claimed_amount").over(w_all_by_claim_date).alias("next_claimed_amount"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_claim_date_month_reported_at)).alias("claimed_amount_change"),
        F.max("claimed_amount").over(w_adjuster_id_by_approved_amou).alias("claimed_amount_max_2_prior"),
        F.round(F.percent_rank().over(w_all_by_claim_date), 3).alias("claim_date_percent_rank"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type), 2).alias("approved_amount_vs_claim_type_avg"),
        F.first("approved_amount_filled").over(w_adjuster_id_by_approved_amou_2).alias("first_approved_amount_filled"),
    )
)

result = (
    metrics
    .filter(F.col("prev_claimed_amount") > 18478.3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "claimed_amount_per_approved_amount",
        "approved_amount_filled",
        "prev_claimed_amount",
        "distinct_claim_type_in_adjuster_id_policy_id",
        "first_known_approved_amount_filled",
        "claim_date_decile",
        "claimed_amount_max_2_prior",
        "claim_date_percent_rank",
        "approved_amount_vs_claim_type_avg",
        "first_approved_amount_filled",
    )
    .orderBy(F.col("approved_amount_vs_claim_type_avg").asc_nulls_last())
)
