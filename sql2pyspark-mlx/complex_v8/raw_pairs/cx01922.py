from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_claim_date_month = Window.partitionBy("claim_type", F.month("claim_date")).orderBy("claimed_amount", "id")
w_claim_type_claim_date_month_2 = Window.partitionBy("claim_type", F.month("claim_date")).orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_claims = (
    df_claims
    .filter(F.col("policy_id") <= 28)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.when(F.col("approved_amount") >= 23120.7, "large").when(F.col("approved_amount") >= 0.0, "medium").otherwise("small").alias("approved_amount_tier"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

with_windows = (
    recent_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "approved_amount_tier",
        F.row_number().over(w_claim_type_claim_date_month).alias("seq_no"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_claim_type_claim_date_month)).alias("approved_amount_change"),
        F.min("claimed_amount").over(w_claim_type_claim_date_month_2).alias("min_claimed_amount_so_far"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

result = (
    derived
    .groupingSets(
        [["policy_id", "claim_type_or_unknown", "seq_no"], ["policy_id"], ["claim_type_or_unknown"], []],
        "policy_id", "claim_type_or_unknown", "seq_no",
    )
    .agg(
        F.grouping("policy_id").alias("is_total_policy_id"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("claim_type_or_unknown").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
    )
)
