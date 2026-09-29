from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.col("status") == "REJECTED") & (F.to_date(F.col("reported_at")) < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
    )
)

tagged = (
    valid
    .select(
        "*",
        F.concat(F.col("status_clean"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_clean_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type_ref")), " ", "_")).alias("claim_type_ref_slug"),
        F.substring(F.trim(F.col("status_clean")), 1, 4).alias("status_clean_prefix"),
    )
)

stats = (
    tagged
    .filter(F.col("claim_date").isNotNull())
    .groupBy(
        F.date_trunc("month", "reported_at").alias("reported_at_month"),
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
        "status",
    )
    .agg(
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.round(100 * F.count(F.when(F.col("claim_type") == "delay", 1)) / F.count("*"), 1).alias("pct_claim_type_delay"),
    )
)

result = stats.select("reported_at_month", "claim_date_ym", "claim_type_list")
