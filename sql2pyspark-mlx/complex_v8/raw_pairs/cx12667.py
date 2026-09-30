from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("premium") >= 2013.8) & (F.col("status") != "LAPSED"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket_2"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

annotated = (
    prepared
    .withColumn("status_or_unknown_prefix", F.substring(F.trim(F.col("status_or_unknown")), 1, 2))
)

exploded_detail_claims = (
    annotated.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.premium_bucket_2",
        "s.status_or_unknown_prefix",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        "c.claim_type",
    )
)

result = (
    exploded_detail_claims
    .select(
        "id",
        "premium",
        "premium_bucket_2",
        "status_or_unknown_prefix",
        "claim_id",
        "claim_date",
        "claim_type",
    )
    .orderBy(F.col("status_or_unknown_prefix").asc_nulls_last())
)
