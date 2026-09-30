from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("reported_at").desc(), F.col("id").desc())

scoped = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
    )
)

latest = (
    scoped
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

annotated = (
    latest
    .filter(F.to_date(F.col("reported_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("status_ref")), " ", "_")).alias("status_ref_slug"),
        F.length(F.trim(F.col("status_ref"))).alias("status_ref_len"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

result = (
    annotated
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "reported_at",
        "claimed_amount_bucket",
        "status_ref_slug",
        "days_since_claim_date",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("adjuster_id").desc())
)
