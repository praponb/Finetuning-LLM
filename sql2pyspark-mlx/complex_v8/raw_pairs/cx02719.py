from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy(F.col("approved_amount_per_claimed_amount").desc(), "id")
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy("approved_amount_per_claimed_amount")
w_status_claim_type_by_approve = Window.partitionBy("status", "claim_type").orderBy("approved_amount_per_claimed_amount")
w_status_claim_type_by_approve_2 = Window.partitionBy("status", "claim_type").orderBy("approved_amount_per_claimed_amount", "id")
w_adjuster_team_by_approved_am = Window.partitionBy("adjuster_team").orderBy("approved_amount_per_claimed_amount", "id")
w_adjuster_id_adjuster_team_by = Window.partitionBy("adjuster_id", "adjuster_team").orderBy("approved_amount_per_claimed_amount", "id")
w_adjuster_team_by_approved_am_2 = Window.partitionBy("adjuster_team").orderBy("approved_amount_per_claimed_amount")
w_policy_id_by_approved_amount_3 = Window.partitionBy("policy_id").orderBy("approved_amount_per_claimed_amount", "id")

valid_claims = (
    df_claims
    .filter(F.col("approved_amount") > 5336.7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        F.when(F.col("approved_amount") >= 22250.6, "HIGH").when(F.col("approved_amount") >= 2349.3, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
    )
)

joined_adjuster = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") >= 2), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        "s.approved_amount_per_claimed_amount",
        F.col("a.team").alias("adjuster_team"),
    )
)

windowed = (
    joined_adjuster
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "status",
        "approved_amount_per_claimed_amount",
        "adjuster_team",
        F.row_number().over(w_policy_id_by_approved_amount).alias("approved_amount_per_claimed_amount_seq"),
        F.rank().over(w_policy_id_by_approved_amount_2).alias("approved_amount_per_claimed_amount_rank"),
        F.round(F.percent_rank().over(w_status_claim_type_by_approve), 3).alias("approved_amount_per_claimed_amount_percent_rank"),
        F.row_number().over(w_status_claim_type_by_approve_2).alias("approved_amount_per_claimed_amount_seq_2"),
        F.row_number().over(w_adjuster_team_by_approved_am).alias("rn"),
        F.dense_rank().over(w_status_claim_type_by_approve).alias("approved_amount_per_claimed_amount_dense_rank"),
        F.ntile(5).over(w_adjuster_id_adjuster_team_by).alias("approved_amount_per_claimed_amount_quintile"),
        F.round(F.cume_dist().over(w_adjuster_team_by_approved_am_2), 3).alias("approved_amount_per_claimed_amount_cume_dist"),
        F.row_number().over(w_policy_id_by_approved_amount_3).alias("approved_amount_per_claimed_amount_seq_3"),
        F.rank().over(w_adjuster_id_adjuster_team_by).alias("approved_amount_per_claimed_amount_rank_2"),
        F.round(F.cume_dist().over(w_policy_id_by_approved_amount_2), 3).alias("approved_amount_per_claimed_amount_cume_dist_2"),
        F.dense_rank().over(w_policy_id_by_approved_amount_2).alias("approved_amount_per_claimed_amount_dense_rank_2"),
    )
)

result = (
    windowed
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "status",
        "approved_amount_per_claimed_amount",
        "adjuster_team",
        "approved_amount_per_claimed_amount_seq",
        "approved_amount_per_claimed_amount_rank",
        "approved_amount_per_claimed_amount_seq_2",
        "rn",
        "approved_amount_per_claimed_amount_seq_3",
        "approved_amount_per_claimed_amount_rank_2",
        "approved_amount_per_claimed_amount_cume_dist_2",
        "approved_amount_per_claimed_amount_dense_rank_2",
        F.abs(F.col("rn") - 5).alias("rn_distance"),
        (F.floor(F.col("rn") / 2) * 2).alias("rn_bucket"),
    )
    .orderBy(F.col("adjuster_team").asc())
)
