from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_reported_at_year = Window.partitionBy("policy_id", F.year("reported_at"))
w_policy_id_adjuster_team_by_r = Window.partitionBy("policy_id", "adjuster_team").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_reported_at_hour = Window.partitionBy("last_reported_at_hour")
w_adjuster_level_last_reported = Window.partitionBy("adjuster_level", "last_reported_at_hour").orderBy("adjuster_level", "id")
w_adjuster_level_last_reported_2 = Window.partitionBy("adjuster_level", "last_reported_at_hour").orderBy("adjuster_level")
w_rn = Window.partitionBy("rn")
w_adjuster_level_dense_rank_by = Window.partitionBy("adjuster_level_dense_rank").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level_dense_rank_by_2 = Window.partitionBy("adjuster_level_dense_rank").orderBy(F.col("adjuster_level").desc(), "id")
w_adjuster_level_dense_rank_by_3 = Window.partitionBy("adjuster_level_dense_rank").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

with_windows = (
    combined_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "adjuster_name",
        "adjuster_team",
        "adjuster_level",
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id_reported_at_year), 2).alias("approved_amount_vs_policy_id_reported_at_year_av"),
        F.max("approved_amount").over(w_policy_id_reported_at_year).alias("policy_id_reported_at_year_max_approved_amount"),
        F.count(F.lit(1)).over(w_policy_id_reported_at_year).alias("rows_in_policy_id_reported_at_year"),
        F.last("reported_at").over(w_policy_id_adjuster_team_by_r).alias("last_reported_at"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "adjuster_id",
        "adjuster_level",
        "approved_amount_vs_policy_id_reported_at_year_av",
        "policy_id_reported_at_year_max_approved_amount",
        "rows_in_policy_id_reported_at_year",
        "last_reported_at",
        F.hour(F.col("last_reported_at")).alias("last_reported_at_hour"),
        F.round(F.col("approved_amount") / F.nullif(F.col("adjuster_level"), F.lit(0)), 3).alias("approved_amount_per_adjuster_level"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "adjuster_level",
        "approved_amount_vs_policy_id_reported_at_year_av",
        "policy_id_reported_at_year_max_approved_amount",
        "rows_in_policy_id_reported_at_year",
        "last_reported_at",
        "last_reported_at_hour",
        "approved_amount_per_adjuster_level",
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_last_reported_at_hour), F.lit(0)), 4).alias("adjuster_level_share_of_last_reported_at_hour"),
        F.row_number().over(w_adjuster_level_last_reported).alias("rn"),
        F.dense_rank().over(w_adjuster_level_last_reported_2).alias("adjuster_level_dense_rank"),
        F.round(F.col("last_reported_at_hour") / F.nullif(F.sum("last_reported_at_hour").over(w_last_reported_at_hour), F.lit(0)), 4).alias("last_reported_at_hour_share_of_last_reported_at_"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "adjuster_level",
        "approved_amount_vs_policy_id_reported_at_year_av",
        "rows_in_policy_id_reported_at_year",
        "last_reported_at",
        "approved_amount_per_adjuster_level",
        "rn",
        "last_reported_at_hour_share_of_last_reported_at_",
        F.round(F.col("last_reported_at_hour") / F.nullif(F.sum("last_reported_at_hour").over(w_rn), F.lit(0)), 4).alias("last_reported_at_hour_share_of_rn"),
        F.first("rn").over(w_adjuster_level_dense_rank_by).alias("first_rn"),
        F.rank().over(w_adjuster_level_dense_rank_by_2).alias("adjuster_level_rank"),
        F.first("adjuster_level_dense_rank", ignorenulls=True).over(w_adjuster_level_dense_rank_by_3).alias("first_known_adjuster_level_dense_rank"),
    )
)

qualified = (
    sequenced
    .crossJoin(sequenced.agg(F.max("first_rn").alias("overall_max_first_rn")))
    .filter(F.col("first_rn") < F.col("overall_max_first_rn"))
    .drop("overall_max_first_rn")
)

result = (
    qualified
    .select(
        "id",
        "adjuster_level",
        "approved_amount_vs_policy_id_reported_at_year_av",
        "rows_in_policy_id_reported_at_year",
        "last_reported_at",
        "approved_amount_per_adjuster_level",
        "rn",
        "last_reported_at_hour_share_of_last_reported_at_",
        "last_reported_at_hour_share_of_rn",
        "first_rn",
        "adjuster_level_rank",
        "first_known_adjuster_level_dense_rank",
        F.abs(F.col("adjuster_level") - 3).alias("adjuster_level_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_reported_at")).alias("days_since_last_reported_at"),
    )
    .orderBy(F.col("rn").desc_nulls_first(), F.col("adjuster_level").desc())
)
