from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_latest = Window.partitionBy("status").orderBy(F.col("greatest_claimed_amount_status_len").desc(), F.col("id").desc())

recent_claims = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("claim_type") == "fire"))
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
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.concat_ws("-", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.max("claim_date").alias("last_claim_date_2"),
    )
)

with_stats = (
    recent_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.last_claim_date", "c.last_claim_date_2")
)

with_windows = (
    with_stats
    .select(
        "*",
        F.size(F.collect_set("claim_type").over(w_policy_id)).alias("distinct_claim_type_in_policy_id"),
        F.size(F.collect_set("status").over(w_policy_id)).alias("distinct_status_in_policy_id"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id), 2).alias("claimed_amount_vs_policy_id_avg"),
        F.sum("approved_amount").over(w_policy_id).alias("policy_id_sum_approved_amount"),
        F.round(F.avg("claimed_amount").over(w_policy_id), 2).alias("policy_id_avg_claimed_amount"),
    )
)

annotated = (
    with_windows
    .filter(F.col("last_claim_date_2").isNotNull())
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "status_prefix",
        "status_len",
        "last_claim_date",
        "claimed_amount_vs_policy_id_avg",
        "policy_id_sum_approved_amount",
        "policy_id_avg_claimed_amount",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.greatest(F.col("claimed_amount"), F.col("status_len")).alias("greatest_claimed_amount_status_len"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

with_parent = (
    annotated.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.status_prefix",
        "s.status_len",
        "s.claimed_amount_vs_policy_id_avg",
        "s.policy_id_sum_approved_amount",
        "s.policy_id_avg_claimed_amount",
        "s.reported_at_date",
        "s.greatest_claimed_amount_status_len",
        "s.claim_type_or_unknown",
        F.col("p.product").alias("policy_product"),
        F.col("p.status").alias("policy_status"),
        F.col("po.region").alias("holder_region"),
    )
)

latest = (
    with_parent
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

detail_rows = (
    latest.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") <= 28426.9), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.status",
        "s.status_len",
        "s.claimed_amount_vs_policy_id_avg",
        "s.policy_id_sum_approved_amount",
        "s.policy_id_avg_claimed_amount",
        "s.reported_at_date",
        "s.claim_type_or_unknown",
        "s.policy_status",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

result = (
    detail_rows
    .rollup("policy_status", "claim_id", "policy_id")
    .agg(
        F.countDistinct("claim_type_or_unknown").alias("distinct_claim_type_or_unknowns"),
    )
)
