from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_all_by_reported_at_moving_3 = Window.orderBy("reported_at", "id").rowsBetween(-3, Window.currentRow)

cleaned_claims = (
    df_claims
    .filter((F.col("claimed_amount") <= 29359.4) & (F.col("reported_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

ranked = (
    cleaned_claims
    .select(
        "*",
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.row_number().over(w_all_by_reported_at).alias("reported_at_seq"),
        F.max("claimed_amount").over(w_all_by_reported_at_moving_3).alias("claimed_amount_max_4"),
        F.sum("claimed_amount").over(w_status).alias("status_sum_claimed_amount"),
        F.size(F.collect_set("claim_type").over(w_status)).alias("distinct_claim_type_in_status"),
    )
)

with_details = (
    ranked.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        "s.reported_at_seq",
        "s.claimed_amount_max_4",
        "s.status_sum_claimed_amount",
        F.col("p.status").alias("policy_status"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
        F.col("po.region").alias("holder_region"),
        F.col("po.joined_on").alias("holder_joined_on"),
    )
)

result = (
    with_details
    .groupBy("policy_id", "reported_at")
    .pivot("policy_status", ["CANCELLED", "EXPIRED", "ACTIVE", "LAPSED"])
    .agg(F.avg("status_sum_claimed_amount"))
    .orderBy("policy_id", "reported_at")
)
