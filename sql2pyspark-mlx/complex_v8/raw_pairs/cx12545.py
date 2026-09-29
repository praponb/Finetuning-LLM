from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_level = Window.partitionBy("adjuster_level")
w_adjuster_team_by_adjuster_le = Window.partitionBy("adjuster_team").orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_team_by_adjuster_le_2 = Window.partitionBy("adjuster_team").orderBy("adjuster_level", "id").rowsBetween(-2, -1)
w_adjuster_level_adjuster_id_b = Window.partitionBy("adjuster_level", "adjuster_id").orderBy("approved_amount", "id")
w_claim_type_by_adjuster_level = Window.partitionBy("claim_type").orderBy("adjuster_level_sum_2_prior", "id")
w_adjuster_id_by_second_approv = Window.partitionBy("adjuster_id").orderBy("second_approved_amount")

scoped_claims = (
    df_claims
    .filter(F.col("status") == "PAID")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
    )
)

labeled = (
    scoped_claims
    .select(
        "*",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
    )
)

with_details = (
    labeled.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.upper(F.trim(F.col("a.adjuster_name"))).like("F%")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.min("adjuster_level").over(w_adjuster_level).alias("adjuster_level_min_adjuster_level"),
        F.nth_value("approved_amount", 2).over(w_adjuster_team_by_adjuster_le).alias("second_approved_amount"),
        F.sum("adjuster_level").over(w_adjuster_team_by_adjuster_le_2).alias("adjuster_level_sum_2_prior"),
        F.lag("approved_amount", 2).over(w_adjuster_level_adjuster_id_b).alias("prev2_approved_amount"),
    )
)

with_windows_2 = (
    with_windows
    .withColumn("adjuster_level_sum_2_prior_rank", F.rank().over(w_claim_type_by_adjuster_level))
    .withColumn("second_approved_amount_rank", F.rank().over(w_adjuster_id_by_second_approv))
)

result = (
    with_windows_2
    .groupBy("adjuster_team", "adjuster_id")
    .pivot("claim_type", ["flood", "fire"])
    .agg(F.avg("adjuster_level_sum_2_prior"))
)
