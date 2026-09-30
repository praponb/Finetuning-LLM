from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id", "status").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())

base = (
    df_claims
    .filter((F.col("adjuster_id") <= 7) & (F.col("reported_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp")))
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
        F.when(F.col("approved_amount") >= 16404.6, "large").when(F.col("approved_amount") >= 0.0, "medium").otherwise("small").alias("approved_amount_tier"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

unique_rows = (
    base
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_parent_policy = (
    unique_rows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.end_date") < F.lit("2026-12-05").cast("date"))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

combined_details = (
    with_parent_policy.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.premium") < 4678.8)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.reported_at",
        "s.policy_end_date",
        "s.policy_start_date",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "policy_end_date",
        "policy_start_date",
    )
    .orderBy(F.col("policy_start_date").desc())
)
