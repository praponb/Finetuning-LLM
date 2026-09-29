from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_claim_type").orderBy(F.col("approved_amount").desc(), F.col("id").desc(), F.col("claim_id").desc())

scoped = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

scored = (
    scoped
    .select(
        "*",
        F.concat_ws(" / ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.when(F.col("approved_amount") >= 32510.7, "large").when(F.col("approved_amount") >= 4234.9, "medium").otherwise("small").alias("approved_amount_tier"),
    )
)

detail_rows_claims = (
    scored.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

deduped = (
    detail_rows_claims
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "policy_id",
        "status",
        "claim_id",
        "claim_approved_amount",
        F.abs(F.col("approved_amount") - 12305.3).alias("approved_amount_distance"),
    )
    .orderBy(F.col("approved_amount_distance").asc_nulls_last())
)
