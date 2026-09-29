from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_policy_premium_prior = Window.orderBy(F.col("policy_premium").desc(), "id", "claim_id").rowsBetween(-3, -1)
w_all_by_policy_premium = Window.orderBy(F.col("policy_premium").desc(), "id", "claim_id")
w_claim_id = Window.partitionBy("claim_id")
w_status_by_policy_premium = Window.partitionBy("status").orderBy(F.col("policy_premium").asc_nulls_last(), "id", "claim_id")

scoped = df_claims.filter(F.col("approved_amount") > 2887.3)

combined_details = (
    scoped.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("p.premium").alias("policy_premium"),
    )
)

line_level_claims = (
    combined_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.policy_premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

metrics = (
    line_level_claims
    .withColumn("policy_premium_avg_3_prior", F.round(F.avg("policy_premium").over(w_all_by_policy_premium_prior), 2))
    .withColumn("next_policy_premium", F.lead("policy_premium").over(w_all_by_policy_premium))
)

with_trends = (
    metrics
    .select(
        "*",
        F.round(F.avg("policy_premium_avg_3_prior").over(w_claim_id), 2).alias("claim_id_avg_policy_premium_avg_3_prior"),
        F.lag("reported_at").over(w_status_by_policy_premium).alias("prev_reported_at"),
        F.ntile(4).over(w_status_by_policy_premium).alias("policy_premium_quartile"),
    )
)

result = (
    with_trends
    .groupBy("reported_at", "policy_premium_quartile")
    .pivot("status", ["IN_REVIEW", "OPEN"])
    .agg(F.sum("policy_premium").alias("amount"), F.count("policy_premium").alias("n"))
    .orderBy("reported_at", "policy_premium_quartile")
)
