from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_or_unknown_by_cla = Window.partitionBy("claim_type_or_unknown").orderBy("claimed_amount")
w_all_by_approved_amount_ties = Window.orderBy("approved_amount")
w_policy_id = Window.partitionBy("policy_id")

cleaned = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.round(F.percent_rank().over(w_claim_type_or_unknown_by_cla), 3).alias("claimed_amount_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_percent_rank"),
        F.max("claimed_amount").over(w_policy_id).alias("policy_id_max_claimed_amount"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("p.status").alias("policy_status"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    joined
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "approved_amount_per_claimed_amount",
        "claim_type_or_unknown",
        "claimed_amount_percent_rank",
        "policy_status",
        "policy_start_date",
        F.abs(F.col("approved_amount") - 14217.6).alias("approved_amount_distance"),
    )
    .orderBy(F.col("claimed_amount").desc(), F.col("reported_at").desc(), "id")
    .limit(10)
)
