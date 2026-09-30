from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("policy_id") >= 25) & (F.col("status") != "APPROVED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

detail_rows_claims = (
    filtered.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status_len",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.status").alias("claim_status"),
    )
)

annotated = (
    detail_rows_claims
    .select(
        "*",
        F.dayofweek(F.col("claim_claim_date")).alias("claim_claim_date_dayofweek"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_claim_date")).alias("days_since_claim_claim_date"),
        F.when(F.col("status_len") >= 11, "large").when(F.col("status_len") >= 6, "medium").otherwise("small").alias("status_len_tier"),
    )
)

line_level_claims = (
    annotated.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (~F.col("c.claim_type").isin("medical", "fire")), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status_len",
        "s.claim_id",
        "s.claim_claim_type",
        "s.claim_claim_date_dayofweek",
        "s.days_since_claim_claim_date",
        "s.status_len_tier",
        F.col("c.id").alias("claim_id_2"),
        "c.claim_date",
        F.col("c.status").alias("claim_status_2"),
        "c.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    line_level_claims
    .groupingSets(
        [["claim_id", "claim_id_2", "claim_claim_date_dayofweek"], ["claim_id"], []],
        "claim_id", "claim_id_2", "claim_claim_date_dayofweek",
    )
    .agg(
        F.min("status_len").alias("min_status_len"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("claim_status_2")).alias("claim_status_2_list"),
    )
    .orderBy(
        F.col("claim_id").asc_nulls_last(),
        F.col("claim_id_2").asc_nulls_last(),
        F.col("claim_claim_date_dayofweek").asc_nulls_last(),
    )
)
