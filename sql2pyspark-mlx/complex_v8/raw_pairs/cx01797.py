from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type_by_repo = Window.partitionBy("policy_id", "claim_type").orderBy("reported_at", "id")
w_adjuster_team = Window.partitionBy("adjuster_team")
w_adjuster_team_status = Window.partitionBy("adjuster_team", "status")
w_adjuster_level_reported_at_m = Window.partitionBy("adjuster_level", F.date_trunc("month", "reported_at")).orderBy("approved_amount", "id").rowsBetween(-1, Window.currentRow)
w_status_by_days_since_claim_d = Window.partitionBy("status").orderBy(F.col("days_since_claim_date").desc(), "id")
w_adjuster_level_by_claim_date = Window.partitionBy("adjuster_level").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_level_reported_at_m_2 = Window.partitionBy("adjuster_level", F.date_trunc("month", "reported_at")).orderBy("approved_amount", "id").rowsBetween(-3, Window.currentRow)

cleaned_claims = (
    df_claims
    .filter((F.col("policy_id") <= 4) & (F.col("approved_amount").between(0.0, 23207.7)))
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
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

with_details = (
    cleaned_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.lead("reported_at").over(w_policy_id_claim_type_by_repo).alias("next_reported_at"),
        F.count(F.lit(1)).over(w_adjuster_team).alias("rows_in_adjuster_team"),
        F.round(F.col("days_since_claim_date") / F.nullif(F.sum("days_since_claim_date").over(w_adjuster_team), F.lit(0)), 4).alias("days_since_claim_date_share_of_adjuster_team"),
        F.size(F.collect_set("adjuster_id").over(w_adjuster_team)).alias("distinct_adjuster_id_in_adjuster_team"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_adjuster_team_status), F.lit(0)), 4).alias("adjuster_level_share_of_adjuster_team_status"),
        F.round(F.avg("adjuster_level").over(w_adjuster_level_reported_at_m), 2).alias("adjuster_level_avg_2"),
        F.dense_rank().over(w_status_by_days_since_claim_d).alias("days_since_claim_date_dense_rank"),
        F.first("approved_amount", ignorenulls=True).over(w_adjuster_level_by_claim_date).alias("first_known_approved_amount"),
        F.round(F.percent_rank().over(w_policy_id_claim_type_by_repo), 3).alias("reported_at_percent_rank"),
        F.size(F.collect_set("claim_type").over(w_adjuster_team)).alias("distinct_claim_type_in_adjuster_team"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_team), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_team"),
        F.round(F.avg("claimed_amount").over(w_adjuster_level_reported_at_m_2), 2).alias("claimed_amount_avg_4"),
    )
)

result = (
    with_windows
    .filter(F.col("reported_at_percent_rank") >= 0.2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "days_since_claim_date",
        "adjuster_name",
        "adjuster_team",
        "rows_in_adjuster_team",
        "distinct_adjuster_id_in_adjuster_team",
        "days_since_claim_date_dense_rank",
        "first_known_approved_amount",
        "reported_at_percent_rank",
        "distinct_claim_type_in_adjuster_team",
        "approved_amount_share_of_adjuster_team",
        "claimed_amount_avg_4",
        F.greatest(F.col("adjuster_level_avg_2"), F.col("adjuster_level")).alias("greatest_adjuster_level_avg_2_adjuster_level"),
        F.to_date(F.col("next_reported_at")).alias("next_reported_at_date"),
    )
    .orderBy("next_reported_at_date", "id")
    .limit(20)
)
