from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status = Window.partitionBy("adjuster_id", "status")

filtered = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-04-27").cast("date")) & (F.col("reported_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        F.when(F.col("approved_amount") >= 14668.0, "A").when(F.col("approved_amount") >= 4554.8, "B").otherwise("C").alias("approved_amount_tier"),
    )
)

ranked = (
    filtered
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "approved_amount_tier",
        F.size(F.collect_set("policy_id").over(w_adjuster_id_status)).alias("distinct_policy_id_in_adjuster_id_status"),
        F.round(F.avg("approved_amount").over(w_adjuster_id_status), 2).alias("adjuster_id_status_avg_approved_amount"),
    )
)

screened = (
    ranked.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
    .filter(F.col("status").isNotNull())
)

without_matches = (
    screened
    .crossJoin(screened.agg(F.avg("distinct_policy_id_in_adjuster_id_status").alias("overall_avg_distinct_policy_id_in_adjuster_id_st")))
    .filter(F.col("distinct_policy_id_in_adjuster_id_status") > F.col("overall_avg_distinct_policy_id_in_adjuster_id_st"))
    .drop("overall_avg_distinct_policy_id_in_adjuster_id_st")
)

tagged = (
    without_matches
    .filter(F.col("distinct_policy_id_in_adjuster_id_status") >= 3)
    .select(
        "id",
        "adjuster_id",
        "approved_amount",
        "approved_amount_tier",
        "distinct_policy_id_in_adjuster_id_status",
        F.coalesce(F.col("approved_amount_tier"), F.lit("UNKNOWN")).alias("approved_amount_tier_or_unknown"),
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        F.concat_ws(": ", F.col("approved_amount_tier"), F.col("status")).alias("approved_amount_tier_status_label"),
    )
)

result = (
    tagged
    .select(
        "id",
        F.col("approved_amount_per_claimed_amount").alias("m_approved_amount_per_claimed_amount"),
        F.col("approved_amount").alias("approved_amount_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_approved_amount_per_claimed_amount", "approved_amount_v"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
