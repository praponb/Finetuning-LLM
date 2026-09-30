from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id = Window.partitionBy("adjuster_id")
w_claim_date_month_adjuster_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy("claim_date")
w_claim_date_month_adjuster_id_2 = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_adjuster_id_3 = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy("claim_date", "id")
w_adjuster_team_adjuster_level = Window.partitionBy("adjuster_team", "adjuster_level").orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_team_adjuster_level_2 = Window.partitionBy("adjuster_team", "adjuster_level").orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_team_adjuster_level_3 = Window.partitionBy("adjuster_team", "adjuster_level").orderBy("adjuster_level", "id").rowsBetween(-6, Window.currentRow)

prepared_claims = (
    df_claims
    .filter(F.col("claimed_amount") < 31752.9)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

joined = (
    prepared_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.length(F.trim(F.col("a.adjuster_name"))) >= 7)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.reported_at",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

with_windows = (
    joined
    .select(
        "id",
        "policy_id",
        "claim_date",
        "adjuster_level",
        "adjuster_team",
        "adjuster_name",
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id), 2).alias("approved_amount_vs_adjuster_id_avg"),
        F.round(F.cume_dist().over(w_claim_date_month_adjuster_id), 3).alias("claim_date_cume_dist"),
        F.count(F.lit(1)).over(w_claim_date_month_adjuster_id_2).alias("running_count"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.lag("approved_amount", 3).over(w_claim_date_month_adjuster_id_3).alias("prev3_approved_amount"),
        F.min("approved_amount").over(w_adjuster_id).alias("adjuster_id_min_approved_amount"),
        F.first("reported_at", ignorenulls=True).over(w_adjuster_team_adjuster_level).alias("first_known_reported_at"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_adjuster_id), F.lit(0)), 4).alias("adjuster_level_share_of_adjuster_id"),
        F.nth_value("approved_amount", 2).over(w_adjuster_team_adjuster_level_2).alias("second_approved_amount"),
        F.round(F.avg("adjuster_level").over(w_adjuster_team_adjuster_level_3), 2).alias("adjuster_level_avg_7"),
        F.round(F.avg("approved_amount").over(w_adjuster_id), 2).alias("adjuster_id_avg_approved_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "claim_date",
        "adjuster_level",
        "adjuster_team",
        "adjuster_name",
        "claim_date_cume_dist",
        "running_count",
        "rows_in_adjuster_id",
        "prev3_approved_amount",
        "adjuster_id_min_approved_amount",
        "first_known_reported_at",
        "adjuster_level_share_of_adjuster_id",
        "second_approved_amount",
        "adjuster_id_avg_approved_amount",
        F.abs(F.col("claim_date_cume_dist") - 0.7).alias("claim_date_cume_dist_distance"),
    )
    .orderBy("id", F.col("second_approved_amount").desc())
)
