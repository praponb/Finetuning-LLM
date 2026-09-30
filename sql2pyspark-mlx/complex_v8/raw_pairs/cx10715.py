from pyspark.sql import functions as F

valid = (
    df_policies
    .filter((F.col("end_date").isNotNull()) | (F.col("holder_id") <= 14))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean_2"),
    )
)

annotated = (
    valid
    .select(
        "id",
        "product",
        "premium",
        "end_date",
        "status",
        "product_clean",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.abs(F.col("premium") - 2033.8).alias("premium_distance"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

tagged = (
    annotated
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("status_or_unknown")), " ", "_")).alias("status_or_unknown_slug"),
        F.coalesce(F.col("days_since_end_date"), F.lit(0)).alias("days_since_end_date_filled"),
        F.round(F.col("premium") / F.nullif(F.col("premium_distance"), F.lit(0)), 3).alias("premium_per_premium_distance"),
    )
)

scored = (
    tagged
    .select(
        "id",
        "product",
        "end_date",
        "status",
        "premium_distance",
        "status_or_unknown_slug",
        "premium_per_premium_distance",
        F.round(F.col("premium_distance") * 0.9, 2).alias("premium_distance_adj"),
        F.round(F.col("premium") / F.nullif(F.col("days_since_end_date"), F.lit(0)), 3).alias("premium_per_days_since_end_date"),
    )
)

result = (
    scored
    .rollup("product", "status")
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
    )
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "row_count",
        "product_list",
    )
    .orderBy(F.col("product").asc_nulls_last(), F.col("status").asc_nulls_last())
)
