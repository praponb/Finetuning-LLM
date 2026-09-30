from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_claims = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")) & (F.col("status").isin("OPEN", "PAID", "IN_REVIEW")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat_ws("-", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
    )
)

sequenced = (
    valid_claims
    .select(
        "*",
        F.row_number().over(w_all_by_claimed_amount).alias("seq_no"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_type_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_claim_type_adjuster_id"),
        F.round(F.percent_rank().over(w_all_by_reported_at), 3).alias("reported_at_percent_rank"),
        F.sum("claimed_amount").over(w_all_by_reported_at_running).alias("running_claimed_amount"),
        F.min("approved_amount").over(w_claim_type_adjuster_id).alias("claim_type_adjuster_id_min_approved_amount"),
        F.round(F.avg("approved_amount").over(w_claim_type_adjuster_id), 2).alias("claim_type_adjuster_id_avg_approved_amount"),
    )
)

scored = (
    sequenced
    .select(
        "*",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        (F.col("running_claimed_amount") - F.col("claimed_amount")).alias("running_claimed_amount_minus_claimed_amount"),
    )
)

scored_2 = (
    scored
    .filter(F.col("seq_no") < 8)
    .select(
        "*",
        F.dayofweek(F.col("reported_at")).alias("reported_at_dayofweek"),
        F.greatest(F.col("claimed_amount"), F.col("running_claimed_amount")).alias("greatest_claimed_amount_running_claimed_amount"),
    )
)

qualified = (
    scored_2.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status").isNotNull()), "left_anti")
    .filter(F.col("claim_type").isNotNull())
)

scored_3 = (
    qualified
    .select(
        "*",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.to_date(F.col("reported_at")).alias("reported_at_date_2"),
        F.concat_ws("|", F.col("status_ref"), F.col("claim_type_status_label")).alias("status_ref_claim_type_status_label_label"),
    )
)

screened = (
    scored_3
    .crossJoin(scored_3.agg(F.avg("approved_amount").alias("overall_avg_approved_amount")))
    .filter(F.col("approved_amount") > F.col("overall_avg_approved_amount"))
    .drop("overall_avg_approved_amount")
)

result = (
    screened
    .filter(F.col("running_claimed_amount") > 175065.0)
    .select("id")
    .distinct()
)
