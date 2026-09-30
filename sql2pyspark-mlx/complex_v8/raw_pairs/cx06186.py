from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_claim_date_month = Window.partitionBy("claim_type", F.date_trunc("month", "claim_date")).orderBy("reported_at", "id")
w_adjuster_team_policy_id = Window.partitionBy("adjuster_team", "policy_id")
w_policy_id_claim_type_by_repo = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("claim_type").orderBy(F.col("last_claim_date").desc(), F.col("id").desc())
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_all_by_claimed_amount_change = Window.orderBy("claimed_amount_change", "id").rowsBetween(-1, Window.currentRow)
w_all_by_claimed_amount_change_2 = Window.orderBy("claimed_amount_change", "id")

prepared = (
    df_claims
    .filter(F.col("policy_id") <= 4)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isNotNull()), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.reported_at",
        "s.least_approved_amount_claimed_amount",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

with_windows = (
    with_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "least_approved_amount_claimed_amount",
        F.ntile(5).over(w_claim_type_claim_date_month).alias("reported_at_quintile"),
        F.max("claimed_amount").over(w_adjuster_team_policy_id).alias("adjuster_team_policy_id_max_claimed_amount"),
        F.lag("reported_at", 2).over(w_claim_type_claim_date_month).alias("prev2_reported_at"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_claim_type_claim_date_month)).alias("claimed_amount_change"),
        F.last("claim_date").over(w_policy_id_claim_type_by_repo).alias("last_claim_date"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_team_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_team_policy_id"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows_2 = (
    latest
    .select(
        "*",
        F.round(F.avg("reported_at_quintile").over(w_adjuster_id_policy_id), 2).alias("adjuster_id_policy_id_avg_reported_at_quintile"),
        F.round(F.avg("reported_at_quintile").over(w_all_by_claimed_amount_change), 2).alias("reported_at_quintile_avg_2"),
        F.row_number().over(w_all_by_claimed_amount_change_2).alias("rn"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount_change_2), 3).alias("claimed_amount_change_percent_rank"),
    )
)

combined_details = (
    with_windows_2.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.least_approved_amount_claimed_amount",
        "s.reported_at_quintile",
        "s.adjuster_team_policy_id_max_claimed_amount",
        "s.claimed_amount_change",
        "s.last_claim_date",
        "s.claimed_amount_share_of_adjuster_team_policy_id",
        "s.adjuster_id_policy_id_avg_reported_at_quintile",
        "s.reported_at_quintile_avg_2",
        "s.rn",
        "a.adjuster_name",
    )
)

result = (
    combined_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "reported_at_quintile",
        "adjuster_team_policy_id_max_claimed_amount",
        "claimed_amount_change",
        "last_claim_date",
        "claimed_amount_share_of_adjuster_team_policy_id",
        "adjuster_id_policy_id_avg_reported_at_quintile",
        "reported_at_quintile_avg_2",
        "rn",
        "adjuster_name",
    )
    .orderBy(F.col("claimed_amount_share_of_adjuster_team_policy_id").asc())
)
