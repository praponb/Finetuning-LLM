from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_policy_id = Window.partitionBy(F.month("claim_date"), "policy_id")
w_status_by_approved_amount_ti = Window.partitionBy("status").orderBy(F.col("approved_amount").desc())
w_status_by_approved_amount = Window.partitionBy("status").orderBy(F.col("approved_amount").desc(), "id")
w_status_by_approved_amount_fu = Window.partitionBy("status").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_claims = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

ranked = (
    base_claims
    .select(
        "*",
        F.max("approved_amount").over(w_claim_date_month_policy_id).alias("claim_date_month_policy_id_max_approved_amount"),
        F.rank().over(w_status_by_approved_amount_ti).alias("approved_amount_rank"),
        F.round(F.percent_rank().over(w_status_by_approved_amount), 3).alias("approved_amount_percent_rank"),
        F.nth_value("approved_amount", 2).over(w_status_by_approved_amount_fu).alias("second_approved_amount"),
    )
)

enriched = (
    ranked
    .select(
        "*",
        F.concat_ws("-", F.col("status"), F.col("status_prefix")).alias("status_status_prefix_label"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
    .filter(F.col("status") == "IN_REVIEW")
)

annotated = (
    filtered
    .select(
        "*",
        F.coalesce(F.col("approved_amount_rank"), F.lit(0)).alias("approved_amount_rank_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("status_prefix")), " ", "_")).alias("status_prefix_slug"),
        F.greatest(F.col("second_approved_amount"), F.col("approved_amount_bucket")).alias("greatest_second_approved_amount_approved_amount_"),
    )
)

result = (
    annotated
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "status_prefix",
        "claim_date_month_policy_id_max_approved_amount",
        "approved_amount_rank",
        "approved_amount_percent_rank",
        "second_approved_amount",
        "status_status_prefix_label",
        "approved_amount_bucket",
        "approved_amount_rank_filled",
        "status_prefix_slug",
        "greatest_second_approved_amount_approved_amount_",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
    .orderBy(F.col("claim_date").asc(), "approved_amount_rank")
)
