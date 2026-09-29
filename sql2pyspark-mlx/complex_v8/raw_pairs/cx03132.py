from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_team = Window.partitionBy("adjuster_team")
w_adjuster_id_adjuster_level_b = Window.partitionBy("adjuster_id", "adjuster_level").orderBy(F.col("adjuster_level").asc_nulls_last(), "id")
w_adjuster_id_adjuster_level_b_2 = Window.partitionBy("adjuster_id", "adjuster_level").orderBy(F.col("adjuster_level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date_moving_6 = Window.orderBy("claim_date", "id").rowsBetween(-6, Window.currentRow)
w_adjuster_id_adjuster_level_b_3 = Window.partitionBy("adjuster_id", "adjuster_level").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_adjuster_id_adjuster_level_b_4 = Window.partitionBy("adjuster_id", "adjuster_level").orderBy(F.col("claim_date").desc(), "id").rowsBetween(-6, -1)
w_adjuster_level_change_adjust = Window.partitionBy("adjuster_level_change", "adjuster_level")
w_next_adjuster_level_by_next = Window.partitionBy("next_adjuster_level").orderBy(F.col("next_adjuster_level").desc())

recent_claims = (
    df_claims
    .filter((F.col("claim_type").isNotNull()) & (F.hour(F.col("reported_at")).between(10, 14)))
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
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
    )
)

with_details_adjuster = (
    recent_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.approved_amount_per_claimed_amount",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

ranked = (
    with_details_adjuster
    .select(
        "*",
        F.sum("approved_amount").over(w_adjuster_team).alias("adjuster_team_sum_approved_amount"),
        F.lead("adjuster_level").over(w_adjuster_id_adjuster_level_b).alias("next_adjuster_level"),
        F.nth_value("claim_date", 2).over(w_adjuster_id_adjuster_level_b_2).alias("second_claim_date"),
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_adjuster_id_adjuster_level_b)).alias("adjuster_level_change"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.sum("adjuster_level_change").over(w_all_by_claim_date_moving_6).alias("adjuster_level_change_sum_7"),
        F.first("approved_amount").over(w_adjuster_id_adjuster_level_b_3).alias("first_approved_amount"),
        F.datediff(F.col("second_claim_date"), F.lag("second_claim_date").over(w_all_by_claim_date)).alias("days_since_prev_second_claim_date"),
        F.sum("claimed_amount").over(w_adjuster_id_adjuster_level_b_4).alias("claimed_amount_sum_6_prior"),
        F.count(F.lit(1)).over(w_adjuster_level_change_adjust).alias("rows_in_adjuster_level_change_adjuster_level"),
        F.round(F.percent_rank().over(w_next_adjuster_level_by_next), 3).alias("next_adjuster_level_percent_rank"),
    )
)

result = (
    windowed
    .groupBy("second_claim_date")
    .pivot("adjuster_team", ["Health", "Travel", "Property"])
    .agg(F.sum("adjuster_team_sum_approved_amount"))
    .orderBy("second_claim_date")
)
