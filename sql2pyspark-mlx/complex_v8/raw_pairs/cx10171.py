from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id", "policy_holder_id").orderBy(F.col("claim_date").desc(), F.col("id").desc(), F.col("claim_id").desc())

base = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(1, 3)) & (~F.col("claim_type").isin("delay", "theft")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
        F.abs(F.col("claimed_amount") - 29336.0).alias("claimed_amount_distance"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.count(F.when(F.col("claim_type") != "medical", 1)).alias("claim_claim_type_count"),
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
    )
)

profiled = (
    base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.first_claim_date",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.total_claim_claimed_amount",
    )
)

joined = (
    profiled.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.end_date").isNotNull()), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.greatest_approved_amount_claimed_amount",
        "s.claimed_amount_distance",
        "s.first_claim_date",
        "s.claim_claim_type_count",
        "s.total_claim_claimed_amount",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

line_level = (
    joined.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.greatest_approved_amount_claimed_amount",
        "s.first_claim_date",
        "s.total_claim_claimed_amount",
        "s.policy_holder_id",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        "c.claim_date",
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

latest_per_key = (
    line_level
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details = (
    latest_per_key.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.policy_holder_id"), "left")
    .select(
        "s.*",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    with_details
    .filter(F.col("total_claim_claimed_amount").between(45085.0, 240020.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "reported_at",
        "greatest_approved_amount_claimed_amount",
        "total_claim_claimed_amount",
        "policy_holder_id",
        "claim_id",
        "claim_claim_type",
        "claim_date",
        "claim_status",
        "holder_region",
        "holder_joined_on",
        (F.col("reported_at") + F.expr("INTERVAL 7 DAYS")).alias("reported_at_plus_7d"),
    )
    .orderBy(
        F.col("greatest_approved_amount_claimed_amount").asc(),
        "holder_joined_on",
    )
)
