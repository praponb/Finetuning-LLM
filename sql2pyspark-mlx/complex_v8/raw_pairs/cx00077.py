from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_id").orderBy("claimed_amount", "id")
w_status_or_unknown_by_approve = Window.partitionBy("status_or_unknown").orderBy("approved_amount", "id")
w_status = Window.partitionBy("status")

filtered = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.concat_ws(" / ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

unique_rows = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    unique_rows
    .select(
        "*",
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_status_or_unknown_by_approve)).alias("days_since_prev_claim_date"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.size(F.collect_set("adjuster_id").over(w_status)).alias("distinct_adjuster_id_in_status"),
        F.min("claimed_amount").over(w_status).alias("status_min_claimed_amount"),
    )
)

result = (
    ranked
    .groupingSets(
        [["status_or_unknown", "status"], ["status_or_unknown"], ["status"]],
        "status_or_unknown", "status",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("status") == "PAID", 1)) / F.count("*"), 1).alias("pct_status_paid"),
    )
    .select(
        F.coalesce(F.col("status_or_unknown"), F.lit("ALL")).alias("status_or_unknown"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "pct_status_paid",
    )
    .orderBy(
        F.col("status_or_unknown").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
