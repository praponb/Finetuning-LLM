from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id").orderBy("claim_date", "id")
w_latest = Window.partitionBy("policy_id").orderBy(F.col("first_claim_date").desc(), F.col("id").desc(), F.col("claim_id").desc())

recent = df_claims.filter(F.col("claim_type").isin("fire", "flood"))

tagged = recent.withColumn("claim_type_clean", F.upper(F.trim(F.col("claim_type"))))

latest_per_key = (
    tagged
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

line_level_claims = (
    latest_per_key.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.to_date(F.col("c.reported_at")) < F.lit("2025-02-18").cast("date")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.min("claim_date").alias("first_claim_date"),
        F.count(F.when(F.col("status").isin("REJECTED", "APPROVED", "OPEN"), 1)).alias("claim_status_count"),
    )
)

with_counts = (
    line_level_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.total_claim_approved_amount",
        "c.first_claim_date",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

deduped = (
    with_counts
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .cube("claim_status_count", "claim_id", "claim_claim_type")
    .agg(
        F.min("claimed_amount").alias("min_claimed_amount"),
        F.count("*").alias("n_rows"),
        F.datediff(F.max("first_claim_date"), F.min("first_claim_date")).alias("first_claim_date_span_days"),
    )
    .select(
        "claim_status_count",
        "claim_id",
        F.coalesce(F.col("claim_claim_type"), F.lit("ALL")).alias("claim_claim_type"),
        "min_claimed_amount",
        "n_rows",
        "first_claim_date_span_days",
    )
    .orderBy(
        F.col("claim_status_count").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
        F.col("claim_claim_type").asc_nulls_last(),
    )
)
