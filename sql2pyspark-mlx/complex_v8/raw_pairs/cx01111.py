from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("holder_id") <= 15) & (F.col("start_date") < F.lit("2025-05-30").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("premium") >= 3532.7, "large").when(F.col("premium") >= 1541.8, "medium").otherwise("small").alias("premium_tier"),
    )
)

annotated = (
    prepared
    .filter(F.col("premium") >= 1042.8)
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

holder_id_stats = (
    annotated
    .groupBy("holder_id")
    .agg(F.max("premium_filled").alias("max_premium_filled"))
)

outliers = (
    annotated.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium_filled") == F.col("g.max_premium_filled"))
    .select(
        "s.*",
        "g.max_premium_filled",
        F.round(F.col("s.premium_filled") - F.col("g.max_premium_filled"), 2).alias("premium_filled_minus_max"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.count(F.when(F.col("product") == "HEALTH", 1)).alias("policy_product_count"),
        F.count("*").alias("policy_count_2"),
    )
)

profiled = (
    outliers.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
    )
)

enriched = (
    profiled
    .filter(F.col("status_ref").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "premium_filled",
        "days_start_date_to_end_date",
        "status_ref",
        "premium_filled_minus_max",
        "policy_product_count",
        "policy_count_2",
        F.coalesce(F.col("policy_count_2"), F.lit(0)).alias("policy_count_2_filled"),
        F.abs(F.col("premium_filled") - 2360.0).alias("premium_filled_distance"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status").isin("ACTIVE", "LAPSED", "EXPIRED")), "left_anti")
    .filter(F.col("premium_filled") < 4280.0)
)

result = (
    without_matches
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium_filled",
        "days_start_date_to_end_date",
        "status_ref",
        "premium_filled_minus_max",
        "policy_product_count",
        "policy_count_2",
        "policy_count_2_filled",
        "premium_filled_distance",
        "product_or_unknown",
        F.length(F.trim(F.col("status_ref"))).alias("status_ref_len"),
        F.least(F.col("policy_count_2_filled"), F.col("premium_filled_distance")).alias("least_policy_count_2_filled_premium_filled_dista"),
    )
    .orderBy(F.col("status_ref").desc())
)
