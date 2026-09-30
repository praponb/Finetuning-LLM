from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

exploded_detail_claims = (
    scoped_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (~F.col("c.claim_type").isin("collision", "fire")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.status_or_unknown",
        "s.status_clean",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
        "c.claim_type",
    )
)

result = (
    exploded_detail_claims
    .groupingSets(
        [["status_or_unknown", "claim_status"], ["status_or_unknown"], ["claim_status"], []],
        "status_or_unknown", "claim_status",
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy(
        F.col("status_or_unknown").asc_nulls_last(),
        F.col("claim_status").asc_nulls_last(),
    )
)
