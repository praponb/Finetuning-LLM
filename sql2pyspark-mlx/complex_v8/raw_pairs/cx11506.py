from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claimed_amount").desc(), F.col("id").desc(), F.col("claim_id").desc())

valid_claims = (
    df_claims
    .filter((F.col("adjuster_id") >= 5) & (F.col("status").isin("IN_REVIEW", "PAID")))
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
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

exploded_detail_claims = (
    valid_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.claim_type_slug",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

deduped = (
    exploded_detail_claims
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .rollup("claim_adjuster_id", "claim_id")
    .agg(
        F.countDistinct("claim_claim_type").alias("distinct_claim_claim_types"),
    )
    .orderBy(
        F.col("claim_adjuster_id").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
    )
)
