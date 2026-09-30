from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id = Window.partitionBy("status", "holder_id")
w_all_by_holder_risk_score = Window.orderBy(F.col("holder_risk_score").desc_nulls_first(), "id")
w_all_by_end_date = Window.orderBy(F.col("end_date").asc_nulls_last(), "id")

cleaned_policys = (
    df_policies
    .filter(F.col("product") != "TRAVEL")
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

joined_holder = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_parent_holder = (
    joined_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.segment") == "RETAIL")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_filled",
        "s.holder_segment",
        "s.holder_risk_score",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment_2"),
    )
)

windowed = (
    with_parent_holder
    .select(
        "*",
        F.sum("premium_filled").over(w_status_holder_id).alias("status_holder_id_sum_premium_filled"),
        F.round(F.percent_rank().over(w_all_by_holder_risk_score), 3).alias("holder_risk_score_percent_rank"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_status_holder_id), F.lit(0)), 4).alias("holder_risk_score_share_of_status_holder_id"),
        F.lag("end_date").over(w_all_by_end_date).alias("prev_end_date"),
    )
)

detail_rows = (
    windowed.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.premium_filled",
        "s.holder_segment_2",
        "s.status_holder_id_sum_premium_filled",
        "s.holder_risk_score_percent_rank",
        "s.holder_risk_score_share_of_status_holder_id",
        "s.prev_end_date",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    detail_rows
    .filter(F.col("claim_id") <= 24)
    .select(
        "id",
        "holder_id",
        "end_date",
        "premium_filled",
        "holder_segment_2",
        "status_holder_id_sum_premium_filled",
        "holder_risk_score_share_of_status_holder_id",
        "prev_end_date",
        "claim_type",
        "claim_approved_amount",
        "claim_claimed_amount",
        F.add_months(F.col("prev_end_date"), 12).alias("prev_end_date_plus_12m"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
    .orderBy(
        F.col("status_holder_id_sum_premium_filled").desc(),
        F.col("prev_end_date").desc(),
    )
)
