from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id").orderBy(F.col("claim_reported_at").desc(), F.col("id").desc(), F.col("claim_id").desc())

scoped = (
    df_claims
    .filter(F.col("claim_type").isin("collision", "fire", "medical"))
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
    )
)

detail_rows = (
    scoped.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_date") < F.lit("2025-09-19").cast("date")), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

latest_per_key = (
    detail_rows
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched = (
    latest_per_key
    .filter(F.col("approved_amount") <= 23120.7)
    .select(
        "*",
        F.round(F.col("approved_amount") * 1.07, 2).alias("approved_amount_adj"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
        (F.col("claim_reported_at") + F.expr("INTERVAL 1 DAYS")).alias("claim_reported_at_plus_1d"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

detail_rows_claims = (
    without_matches.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.claim_id",
        "s.claim_claim_type",
        "s.claim_reported_at",
        "s.approved_amount_bucket",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    detail_rows_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "claim_id",
        "claim_claim_type",
        "claim_id_2",
        "claim_approved_amount",
        F.abs(F.col("approved_amount") - 14217.6).alias("approved_amount_distance"),
        F.upper(F.trim(F.col("claim_status"))).alias("claim_status_clean"),
    )
)
