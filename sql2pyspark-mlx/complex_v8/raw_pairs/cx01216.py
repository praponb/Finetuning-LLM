from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy("approved_amount_filled", "id")
w_policy_id = Window.partitionBy("policy_id")
w_status_claim_type_by_approve = Window.partitionBy("status", "claim_type").orderBy(F.col("approved_amount_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_approved_amou_2 = Window.partitionBy("adjuster_id").orderBy("approved_amount_filled", "id").rowsBetween(-3, Window.currentRow)
w_all_by_approved_amount_fille = Window.orderBy(F.col("approved_amount_filled").desc(), "id")
w_status_claim_type_by_approve_2 = Window.partitionBy("status", "claim_type").orderBy(F.col("approved_amount_filled").desc(), "id")
w_claim_date_year_by_approved = Window.partitionBy(F.year("claim_date")).orderBy("approved_amount")
w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy(F.col("approved_amount_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_approved_amou_3 = Window.partitionBy("adjuster_id").orderBy("approved_amount_filled", "id").rowsBetween(-6, -1)
w_claim_date_year_by_approved_2 = Window.partitionBy(F.year("claim_date")).orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy(F.col("approved_amount_filled").desc(), "id")
w_claim_date_year_by_approved_3 = Window.partitionBy(F.year("claim_date")).orderBy("approved_amount", "id").rowsBetween(-6, -1)

valid = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-03-31").cast("date")) & (F.col("reported_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

enriched_adjuster = (
    valid.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.approved_amount_filled",
        "s.status_slug",
        "a.adjuster_name",
    )
)

ranked = (
    enriched_adjuster
    .select(
        "*",
        F.row_number().over(w_adjuster_id_by_approved_amou).alias("rn"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.nth_value("approved_amount_filled", 2).over(w_status_claim_type_by_approve).alias("second_approved_amount_filled"),
        F.round(F.avg("approved_amount_filled").over(w_adjuster_id_by_approved_amou_2), 2).alias("approved_amount_filled_avg_4"),
        F.row_number().over(w_all_by_approved_amount_fille).alias("rn_2"),
        (F.col("approved_amount_filled") - F.lag("approved_amount_filled").over(w_status_claim_type_by_approve_2)).alias("approved_amount_filled_change"),
        F.round(F.avg("approved_amount_filled").over(w_policy_id), 2).alias("policy_id_avg_approved_amount_filled"),
        F.round(F.percent_rank().over(w_claim_date_year_by_approved), 3).alias("approved_amount_percent_rank"),
        F.first("approved_amount").over(w_policy_id_by_approved_amount).alias("first_approved_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id"),
        F.sum("approved_amount_filled").over(w_adjuster_id_by_approved_amou_3).alias("approved_amount_filled_sum_6_prior"),
        F.lead("approved_amount").over(w_status_claim_type_by_approve_2).alias("next_approved_amount"),
        F.sum("approved_amount").over(w_claim_date_year_by_approved_2).alias("running_approved_amount"),
        F.round(F.cume_dist().over(w_all_by_approved_amount_fille), 3).alias("approved_amount_filled_cume_dist"),
        F.lead("claim_date", 3).over(w_policy_id_by_approved_amount_2).alias("next3_claim_date"),
        F.max("approved_amount").over(w_claim_date_year_by_approved_3).alias("approved_amount_max_6_prior"),
    )
)

first_rows = ranked.filter(F.col("rn_2") == 1)

result = (
    first_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "approved_amount_filled",
        "status_slug",
        "adjuster_name",
        "rn",
        "second_approved_amount_filled",
        "approved_amount_filled_avg_4",
        "rn_2",
        "policy_id_avg_approved_amount_filled",
        "first_approved_amount",
        "approved_amount_share_of_policy_id",
        "approved_amount_filled_sum_6_prior",
        "next_approved_amount",
        "approved_amount_filled_cume_dist",
        "approved_amount_max_6_prior",
        F.substring(F.trim(F.col("adjuster_name")), 1, 2).alias("adjuster_name_prefix"),
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
    )
    .orderBy(
        F.col("approved_amount_filled_avg_4").asc(),
        F.col("next_approved_amount").desc_nulls_first(),
        "id",
    )
    .limit(5)
)
