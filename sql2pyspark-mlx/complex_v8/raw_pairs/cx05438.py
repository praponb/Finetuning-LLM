from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id", "claim_type").orderBy(F.col("claim_date").desc(), F.col("id").desc())

recent_claims = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) | (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.min("reported_at").alias("first_claim_reported_at"))
)

with_counts = (
    recent_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.first_claim_reported_at")
)

deduped = (
    with_counts
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .groupBy("claim_date")
    .pivot("status", ["APPROVED", "REJECTED"])
    .agg(F.sum("days_since_claim_date"))
    .orderBy("claim_date")
)
