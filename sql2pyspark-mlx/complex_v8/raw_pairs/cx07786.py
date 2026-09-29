from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_level_adjuster_id_b = Window.partitionBy("adjuster_level", "adjuster_id").orderBy(F.col("approved_amount").desc(), "id")
w_adjuster_level_adjuster_id_b_2 = Window.partitionBy("adjuster_level", "adjuster_id").orderBy(F.col("approved_amount").asc_nulls_last(), "id")
w_adjuster_team_adjuster_level = Window.partitionBy("adjuster_team", "adjuster_level").orderBy("approved_amount", "id")
w_claim_date_month_policy_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "policy_id")
w_adjuster_level_adjuster_id_b_3 = Window.partitionBy("adjuster_level", "adjuster_id").orderBy(F.col("approved_amount").desc())
w_adjuster_level_adjuster_id_b_4 = Window.partitionBy("adjuster_level", "adjuster_id").orderBy(F.col("approved_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
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
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

combined_details_adjuster = (
    recent.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

windowed = (
    combined_details_adjuster
    .select(
        "*",
        F.lead("claim_date", 2).over(w_adjuster_level_adjuster_id_b).alias("next2_claim_date"),
        F.row_number().over(w_adjuster_level_adjuster_id_b_2).alias("rn"),
        F.lag("adjuster_level", 3).over(w_adjuster_team_adjuster_level).alias("prev3_adjuster_level"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_adjuster_level_adjuster_id_b)).alias("days_since_prev_claim_date"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_claim_date_month_policy_id), 2).alias("adjuster_level_vs_claim_date_month_policy_id_avg"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_claim_date_month_policy_id), F.lit(0)), 4).alias("adjuster_level_share_of_claim_date_month_policy_"),
        F.round(F.percent_rank().over(w_adjuster_level_adjuster_id_b_3), 3).alias("approved_amount_percent_rank"),
        F.last("approved_amount", ignorenulls=True).over(w_adjuster_level_adjuster_id_b_4).alias("last_known_approved_amount"),
    )
)

top_rows = windowed.filter(F.col("rn") == 1)

result = (
    top_rows
    .filter(F.col("days_since_prev_claim_date") > 30)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "adjuster_team",
        "adjuster_level",
        "next2_claim_date",
        "prev3_adjuster_level",
        "days_since_prev_claim_date",
        "adjuster_level_vs_claim_date_month_policy_id_avg",
        "approved_amount_percent_rank",
    )
    .orderBy("claimed_amount", "adjuster_level_vs_claim_date_month_policy_id_avg")
)
