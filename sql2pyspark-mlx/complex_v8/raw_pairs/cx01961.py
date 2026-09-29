from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status_or_unknown").orderBy(F.col("last_claim_date").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("claim_type").orderBy("claim_claim_date_2", "id", "claim_id", "claim_id_2")
w_dedup_3 = Window.partitionBy("claim_type", "adjuster_id").orderBy(F.col("claim_claim_date").desc(), F.col("id").desc(), F.col("claim_id").desc(), F.col("claim_id_2").desc())

cleaned = (
    df_claims
    .filter(F.col("claimed_amount") < 43724.9)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.concat_ws(": ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.max("claim_date").alias("last_claim_date"))
)

with_counts = (
    cleaned.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.last_claim_date")
)

latest_per_key = (
    with_counts
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_children = (
    latest_per_key.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.reported_at",
        "s.status_claim_type_label",
        "s.last_claim_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

exploded_detail_claims = (
    with_children.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.claim_adjuster_id")) & (F.col("c.claim_date").between(F.lit("2024-03-30").cast("date"), F.lit("2025-11-12").cast("date"))), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.reported_at",
        "s.last_claim_date",
        "s.claim_id",
        "s.claim_approved_amount",
        "s.claim_claim_date",
        "s.claim_adjuster_id",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.claim_date").alias("claim_claim_date_2"),
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
    )
)

deduped = (
    exploded_detail_claims
    .withColumn("recency_rank", F.row_number().over(w_dedup_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

latest = (
    deduped
    .withColumn("recency_rank", F.row_number().over(w_dedup_3))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "id",
        "policy_id",
        "claim_type",
        "reported_at",
        "last_claim_date",
        "claim_id",
        "claim_approved_amount",
        "claim_claim_date",
        "claim_claim_date_2",
        "claim_approved_amount_2",
    )
    .orderBy("id")
)
