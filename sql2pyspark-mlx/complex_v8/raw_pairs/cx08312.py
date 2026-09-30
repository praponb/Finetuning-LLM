from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("status", "policy_holder_id").orderBy("claimed_amount", "id")

cleaned_claims = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("claim_type") == "flood"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("claimed_amount") * 0.25, 2).alias("claimed_amount_adj"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("status") == "APPROVED", 1)).alias("claim_status_count"),
        F.count(F.when(F.col("claim_type") != "delay", 1)).alias("claim_claim_type_count"),
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
    )
)

summarized_rows = (
    cleaned_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.total_claim_claimed_amount",
    )
)

with_details_policy = (
    summarized_rows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_adj",
        "s.claim_status_count",
        "s.total_claim_claimed_amount",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.status").alias("policy_status"),
    )
)

latest = (
    with_details_policy
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select("id", "claimed_amount_adj", "total_claim_claimed_amount")
    .unpivot(
        ids=["id"],
        values=["claimed_amount_adj", "total_claim_claimed_amount"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "measure")
)
