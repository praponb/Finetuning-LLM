from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_type").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())

prepared_claims = (
    df_claims
    .filter(F.col("claimed_amount") <= 40639.7)
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
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
    )
)

latest_per_key = (
    prepared_claims
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_children = (
    latest_per_key.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status").isin("IN_REVIEW", "APPROVED", "REJECTED")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_type_prefix",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

result = (
    with_children
    .groupingSets(
        [["claim_policy_id", "adjuster_id"], ["claim_policy_id"], []],
        "claim_policy_id", "adjuster_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.col("claim_status") == "OPEN", F.col("approved_amount"))).alias("approved_amount_claim_status_open"),
    )
    .orderBy(
        F.col("claim_policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
