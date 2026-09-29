from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_status_by_claim_date = Window.partitionBy("status").orderBy(F.col("claim_date").desc(), "id")
w_policy_id_by_claim_date_full = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_claim_date_runn = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_status = Window.partitionBy("status")
w_all_by_claimed_amount_ties = Window.orderBy("claimed_amount")
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy("claim_date", "id")
w_all_by_claimed_amount_runnin = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc_nulls_first())

cleaned = (
    df_claims
    .filter(F.col("claim_type") == "delay")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

with_trends = (
    cleaned
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "claimed_amount_filled",
        F.lag("reported_at").over(w_all_by_claimed_amount).alias("prev_reported_at"),
        F.row_number().over(w_all_by_claimed_amount).alias("seq_no"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_status_by_claim_date)).alias("approved_amount_change"),
        F.first("claimed_amount_filled").over(w_policy_id_by_claim_date_full).alias("first_claimed_amount_filled"),
        F.last("claimed_amount_filled", ignorenulls=True).over(w_policy_id_by_claim_date_runn).alias("last_known_claimed_amount_filled"),
        F.lead("approved_amount").over(w_all_by_claimed_amount).alias("next_approved_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id_policy_id"),
        F.min("claimed_amount").over(w_status).alias("status_min_claimed_amount"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_cume_dist"),
        F.ntile(10).over(w_all_by_claimed_amount).alias("claimed_amount_decile"),
        F.min("approved_amount").over(w_adjuster_id_policy_id).alias("adjuster_id_policy_id_min_approved_amount"),
        F.round(F.percent_rank().over(w_policy_id_by_claim_date), 3).alias("claim_date_percent_rank"),
        F.last("reported_at", ignorenulls=True).over(w_all_by_claimed_amount_runnin).alias("last_known_reported_at"),
        F.rank().over(w_policy_id_by_claim_date).alias("claim_date_rank"),
        F.rank().over(w_all_by_claim_date_ties).alias("claim_date_rank_2"),
    )
)

scored = (
    with_trends
    .select(
        "*",
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

result = (
    scored
    .filter(F.col("claimed_amount_filled") >= 7585.0)
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "prev_reported_at",
        "last_known_claimed_amount_filled",
        "next_approved_amount",
        "approved_amount_share_of_adjuster_id_policy_id",
        "status_min_claimed_amount",
        "claimed_amount_cume_dist",
        "claimed_amount_decile",
        "adjuster_id_policy_id_min_approved_amount",
        "last_known_reported_at",
        "claim_date_rank",
        "claim_date_rank_2",
        "claim_type_clean",
        "claim_type_len",
        F.round(F.col("claim_date_rank") / F.nullif(F.col("claimed_amount_cume_dist"), F.lit(0)), 3).alias("claim_date_rank_per_claimed_amount_cume_dist"),
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
    )
    .orderBy("claimed_amount_cume_dist")
)
