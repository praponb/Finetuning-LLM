from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_product", "claim_adjuster_id").orderBy(F.col("adjuster_level").desc(), F.col("id").desc(), F.col("claim_id").desc())

base_claims = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

exploded_detail_claims = (
    base_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.hour(F.col("c.reported_at")).between(9, 11)), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status",
        "s.claim_type_len",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

with_parent_policy = (
    exploded_detail_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type_len",
        "s.claim_id",
        "s.claim_status",
        "s.claim_adjuster_id",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

joined_adjuster = (
    with_parent_policy.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type_len",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.policy_holder_id",
        "s.policy_product",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

qualified = (
    joined_adjuster
    .crossJoin(joined_adjuster.agg(F.avg("adjuster_level").alias("overall_avg_adjuster_level")))
    .filter(F.col("adjuster_level") > F.col("overall_avg_adjuster_level"))
    .drop("overall_avg_adjuster_level")
)

latest_per_key = (
    qualified
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined_policy = (
    latest_per_key.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
        F.col("p.holder_id").alias("policy_holder_id_2"),
    )
)

result = (
    joined_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type_len",
        "claim_id",
        "claim_adjuster_id",
        "policy_holder_id",
        "policy_product",
        "adjuster_level",
        "adjuster_name",
        "adjuster_team",
        "policy_premium",
        "policy_status",
    )
    .orderBy(F.col("adjuster_team").desc(), "id", "claim_id")
    .limit(20)
)
