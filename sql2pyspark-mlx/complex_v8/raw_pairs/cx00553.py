from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id")
w_policy_end_date_month_by_app = Window.partitionBy(F.month("policy_end_date")).orderBy("approved_amount", "id").rowsBetween(-6, -1)
w_policy_id_reported_at_year_b = Window.partitionBy("policy_id", F.year("reported_at")).orderBy(F.col("policy_premium").desc(), "id")
w_adjuster_id_reported_at_year = Window.partitionBy("adjuster_id", F.year("reported_at")).orderBy(F.col("policy_end_date").desc(), "id")
w_policy_holder_id_claim_type = Window.partitionBy("policy_holder_id", "claim_type")
w_reported_at_month_claim_type = Window.partitionBy(F.month("reported_at"), "claim_type")
w_policy_id_reported_at_year_b_2 = Window.partitionBy("policy_id", F.year("reported_at")).orderBy(F.col("policy_premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_reported_at_year_b_3 = Window.partitionBy("policy_id", F.year("reported_at")).orderBy(F.col("policy_premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_approved_amou_2 = Window.partitionBy("adjuster_id").orderBy("approved_amount")
w_adjuster_id_reported_at_year_2 = Window.partitionBy("adjuster_id", F.year("reported_at")).orderBy(F.col("policy_end_date").desc(), "id").rowsBetween(-3, Window.currentRow)

prepared = (
    df_claims
    .filter((F.col("approved_amount") >= 2349.3) & (F.col("claim_date").between(F.lit("2024-06-20").cast("date"), F.lit("2025-04-11").cast("date"))))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

combined_details_policy = (
    prepared.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.premium") <= 4417.0)
    .select(
        "s.*",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

metrics = (
    combined_details_policy
    .select(
        "*",
        F.lead("claimed_amount_filled", 3).over(w_adjuster_id_by_approved_amou).alias("next3_claimed_amount_filled"),
        F.row_number().over(w_adjuster_id_by_approved_amou).alias("seq_no"),
        F.round(F.avg("claimed_amount_filled").over(w_policy_end_date_month_by_app), 2).alias("claimed_amount_filled_avg_6_prior"),
        F.lag("reported_at").over(w_policy_id_reported_at_year_b).alias("prev_reported_at"),
        F.row_number().over(w_adjuster_id_reported_at_year).alias("rn"),
        F.round(F.percent_rank().over(w_adjuster_id_reported_at_year), 3).alias("policy_end_date_percent_rank"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_holder_id_claim_type), F.lit(0)), 4).alias("claimed_amount_share_of_policy_holder_id_claim_t"),
        F.count(F.lit(1)).over(w_reported_at_month_claim_type).alias("rows_in_reported_at_month_claim_type"),
        F.ntile(10).over(w_adjuster_id_by_approved_amou).alias("approved_amount_decile"),
        F.last("reported_at").over(w_policy_id_reported_at_year_b_2).alias("last_reported_at"),
        F.min("policy_premium").over(w_policy_holder_id_claim_type).alias("policy_holder_id_claim_type_min_policy_premium"),
        F.ntile(3).over(w_adjuster_id_reported_at_year).alias("policy_end_date_tercile"),
        F.first("approved_amount", ignorenulls=True).over(w_policy_id_reported_at_year_b_3).alias("first_known_approved_amount"),
        F.rank().over(w_adjuster_id_by_approved_amou_2).alias("approved_amount_rank"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_reported_at_year_2), 2).alias("claimed_amount_avg_4"),
    )
)

result = (
    metrics
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claimed_amount_filled",
        "policy_premium",
        "policy_end_date",
        "policy_holder_id",
        "seq_no",
        "claimed_amount_filled_avg_6_prior",
        "prev_reported_at",
        "rn",
        "policy_end_date_percent_rank",
        "claimed_amount_share_of_policy_holder_id_claim_t",
        "rows_in_reported_at_month_claim_type",
        "approved_amount_decile",
        "last_reported_at",
        "policy_holder_id_claim_type_min_policy_premium",
        "first_known_approved_amount",
        "approved_amount_rank",
        "claimed_amount_avg_4",
    )
    .orderBy(F.col("seq_no").desc_nulls_first(), F.col("last_reported_at").desc())
)
