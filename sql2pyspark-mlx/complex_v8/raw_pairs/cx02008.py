from pyspark.sql import functions as F

recent = (
    df_claims
    .filter((~F.col("status").isin("PAID", "APPROVED")) & (F.col("claimed_amount") <= 31715.7))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        F.round(F.col("claimed_amount") * 1.5, 2).alias("claimed_amount_adj"),
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

derived = (
    recent
    .filter(F.col("claim_date") < F.lit("2024-12-20").cast("date"))
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "reported_at",
        "claimed_amount_adj",
        "claim_type_clean",
        F.substring(F.trim(F.col("claim_type_clean")), 1, 4).alias("claim_type_clean_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type_clean")), " ", "_")).alias("claim_type_clean_slug"),
    )
)

result = (
    derived
    .rollup("adjuster_id", "claim_type")
    .agg(
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.count("*").alias("cnt"),
        F.max("reported_at").alias("last_reported_at"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
