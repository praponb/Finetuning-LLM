from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date_month_full = Window.orderBy("claim_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date_month_ties = Window.orderBy("claim_date_month")
w_all_by_claim_date_month = Window.orderBy("claim_date_month", "id")
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-02-18 13:00:00").cast("timestamp")) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
    )
)

joined_adjuster = (
    recent.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        "s.reported_at",
        "s.claim_date_month",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

ranked = (
    joined_adjuster
    .select(
        "*",
        F.last("reported_at").over(w_all_by_claim_date_month_full).alias("last_reported_at"),
        F.dense_rank().over(w_all_by_claim_date_month_ties).alias("claim_date_month_dense_rank"),
        F.round((F.unix_timestamp("claim_date_month") - F.unix_timestamp(F.lag("claim_date_month").over(w_all_by_claim_date_month))) / 60, 1).alias("minutes_since_prev_claim_date_month"),
        F.nth_value("reported_at", 2).over(w_all_by_claim_date_month_full).alias("second_reported_at"),
        F.dense_rank().over(w_all_by_claim_date_month).alias("claim_date_month_dense_rank_2"),
        F.rank().over(w_all_by_reported_at_ties).alias("reported_at_rank"),
        F.round(F.cume_dist().over(w_all_by_claim_date_month_ties), 3).alias("claim_date_month_cume_dist"),
        F.last("reported_at", ignorenulls=True).over(w_all_by_reported_at_running).alias("last_known_reported_at"),
        F.lead("reported_at").over(w_all_by_claim_date_month).alias("next_reported_at"),
        F.row_number().over(w_all_by_claim_date_month).alias("rn"),
        F.round(F.percent_rank().over(w_all_by_reported_at_ties), 3).alias("reported_at_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_claim_date_month_ties), 3).alias("claim_date_month_percent_rank"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_all_by_claim_date_month))) / 60, 1).alias("minutes_since_prev_reported_at"),
    )
)

result = (
    ranked
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "reported_at",
        "claim_date_month",
        "adjuster_name",
        "last_reported_at",
        "claim_date_month_dense_rank",
        "minutes_since_prev_claim_date_month",
        "claim_date_month_dense_rank_2",
        "reported_at_rank",
        "last_known_reported_at",
        "next_reported_at",
        "rn",
        "reported_at_percent_rank",
        "claim_date_month_percent_rank",
        F.year(F.col("second_reported_at")).alias("second_reported_at_year"),
        F.when(F.col("rn") >= 6, "HIGH").when(F.col("rn") >= 3, "MEDIUM").otherwise("LOW").alias("rn_tier"),
    )
    .orderBy(F.col("adjuster_name").asc(), F.col("rn").asc(), "id")
    .limit(5)
)
