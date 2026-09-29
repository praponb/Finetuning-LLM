from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")
w_status_adjuster_id_by_approv = Window.partitionBy("status", "adjuster_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_claim_date = Window.partitionBy("status").orderBy("claim_date", "id")
w_policy_id_by_reported_at_tie = Window.partitionBy("policy_id").orderBy(F.col("reported_at").asc_nulls_last())
w_claim_date_month_status_by_c = Window.partitionBy(F.month("claim_date"), "status").orderBy("claim_date", "id")
w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy(F.col("reported_at").asc_nulls_last(), "id")
w_status_adjuster_id_by_approv_2 = Window.partitionBy("status", "adjuster_id").orderBy(F.col("approved_amount").desc(), "id")
w_reported_at_year = Window.partitionBy(F.year("reported_at"))
w_approved_amount_tercile_fill = Window.partitionBy("approved_amount_tercile_filled", "reported_at_dense_rank_distance").orderBy("reported_at_dense_rank", "id")
w_policy_id = Window.partitionBy("policy_id")
w_status_by_approved_amount_sh = Window.partitionBy("status").orderBy(F.col("approved_amount_share_of_reported_at_year").desc(), "id")

recent = df_claims.filter(F.col("policy_id") <= 19)

windowed = (
    recent
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "status",
        F.count(F.lit(1)).over(w_claim_type_adjuster_id).alias("rows_in_claim_type_adjuster_id"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_type_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_claim_type_adjuster_id"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_claim_type_adjuster_id"),
        F.sum("claimed_amount").over(w_status_adjuster_id_by_approv).alias("running_claimed_amount"),
        F.lead("reported_at", 2).over(w_status_by_claim_date).alias("next2_reported_at"),
        F.dense_rank().over(w_policy_id_by_reported_at_tie).alias("reported_at_dense_rank"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_claim_date_month_status_by_c))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.lead("approved_amount", 2).over(w_policy_id_by_reported_at).alias("next2_approved_amount"),
        F.ntile(3).over(w_status_adjuster_id_by_approv_2).alias("approved_amount_tercile"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_reported_at_year), 2).alias("claimed_amount_vs_reported_at_year_avg"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_reported_at_year), F.lit(0)), 4).alias("approved_amount_share_of_reported_at_year"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_reported_at_year), 2).alias("approved_amount_vs_reported_at_year_avg"),
    )
)

scored = (
    windowed
    .select(
        "*",
        F.coalesce(F.col("approved_amount_tercile"), F.lit(0)).alias("approved_amount_tercile_filled"),
        F.abs(F.col("reported_at_dense_rank") - 5).alias("reported_at_dense_rank_distance"),
    )
)

sequenced = (
    scored
    .select(
        "*",
        F.row_number().over(w_approved_amount_tercile_fill).alias("rn"),
        F.max("next2_approved_amount").over(w_policy_id).alias("policy_id_max_next2_approved_amount"),
        F.ntile(5).over(w_approved_amount_tercile_fill).alias("reported_at_dense_rank_quintile"),
        F.ntile(5).over(w_status_by_approved_amount_sh).alias("approved_amount_share_of_reported_at_year_quinti"),
    )
)

summary = (
    sequenced
    .filter(F.col("reported_at_dense_rank_distance") > 4)
    .groupBy("status", "policy_id")
    .agg(
        F.round(F.avg("approved_amount_share_of_claim_type_adjuster_id"), 2).alias("avg_approved_amount_share_of_claim_type_adjuster"),
        F.round(F.sum("approved_amount_tercile") / F.nullif(F.sum("claimed_amount_share_of_claim_type_adjuster_id"), F.lit(0)), 3).alias("approved_amount_tercile_to_claimed_amount_share_"),
    )
)

result = (
    summary
    .select("status", "policy_id", "approved_amount_tercile_to_claimed_amount_share_")
    .orderBy(
        F.col("policy_id").desc(),
        "approved_amount_tercile_to_claimed_amount_share_",
    )
)
