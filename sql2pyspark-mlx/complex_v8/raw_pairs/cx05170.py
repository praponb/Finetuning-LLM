from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_type").orderBy(F.col("claim_claim_type_count_2").desc(), F.col("id").desc())

prepared = (
    df_claims
    .filter((F.col("claim_type") == "flood") & (F.col("status") == "APPROVED"))
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
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
        F.when(F.col("approved_amount") >= 22250.6, "large").when(F.col("approved_amount") >= 4554.8, "medium").otherwise("small").alias("approved_amount_tier"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("claim_claim_type_count"),
        F.count(F.when(F.col("claim_type").isin("delay", "flood"), 1)).alias("claim_claim_type_count_2"),
        F.count("*").alias("claim_count"),
    )
)

with_stats = (
    prepared.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        F.coalesce(F.col("c.claim_claim_type_count_2"), F.lit(0)).alias("claim_claim_type_count_2"),
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

latest_per_key = (
    with_stats
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

detail_rows_claims = (
    latest_per_key.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date") < F.lit("2025-01-04").cast("date")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.approved_amount_minus_claimed_amount",
        "s.approved_amount_tier",
        "s.claim_claim_type_count",
        "s.claim_count",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    detail_rows_claims
    .select(
        "id",
        "claim_id",
        "claim_claim_type_count",
        "claim_count",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["claim_claim_type_count", "claim_count"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
