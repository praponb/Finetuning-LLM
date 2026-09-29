from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id").orderBy("last_claim_date", "id")

valid_claims = (
    df_claims
    .filter((F.col("claimed_amount") <= 42447.2) & (F.col("reported_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

tagged = (
    valid_claims
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("claim_date").alias("last_claim_date"),
        F.count("*").alias("claim_count_2"),
    )
)

with_stats = (
    tagged.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

unique_rows = (
    with_stats
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claim_type_slug",
        "last_claim_date",
        "claim_count_2",
    )
)
