from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("claim_date").between(F.lit("2024-06-13").cast("date"), F.lit("2025-05-30").cast("date"))) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.round(F.col("claimed_amount") * 0.9, 2).alias("claimed_amount_adj"),
        F.when(F.col("status").isin("PAID", "REJECTED"), "group_a").when(F.col("status") == "OPEN", "group_b").otherwise("other").alias("status_group"),
    )
)

enriched = (
    filtered_claims
    .filter(F.col("adjuster_id").isNotNull())
    .withColumn("reported_at_date", F.to_date(F.col("reported_at")))
)

result = (
    enriched
    .rollup("adjuster_id", "status_group", "policy_id")
    .agg(
        F.countDistinct("status_group").alias("distinct_status_groups"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("status_group").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
