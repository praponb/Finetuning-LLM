from pyspark.sql import functions as F

scoped = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.round(F.col("claimed_amount") * 1.07, 2).alias("claimed_amount_adj"),
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"))
)

with_counts = (
    scoped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

filtered = (
    with_counts.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") < 26237.1), "left_semi")
)

result = (
    filtered
    .select("reported_at", "claimed_amount_adj", "id")
    .distinct()
    .orderBy(F.col("id").desc(), F.col("claimed_amount_adj").desc())
)
