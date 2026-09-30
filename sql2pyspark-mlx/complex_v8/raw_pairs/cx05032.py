from pyspark.sql import functions as F

recent = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

summary = (
    recent
    .filter(F.col("days_since_start_date").between(35, 455))
    .groupBy("product")
    .agg(
        F.count("*").alias("n_rows"),
        F.min("days_since_start_date").alias("min_days_since_start_date"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.round(F.sum("days_since_start_date") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("days_since_start_date_to_premium_ratio"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
    )
)

scored = (
    summary
    .filter(F.col("stddev_premium") > 2000.0)
    .select(
        "product",
        "n_rows",
        "min_days_since_start_date",
        "product_list",
        "days_since_start_date_to_premium_ratio",
        F.greatest(F.col("n_rows"), F.col("stddev_premium")).alias("greatest_n_rows_stddev_premium"),
    )
)

annotated = (
    scored
    .select(
        "product",
        "n_rows",
        "min_days_since_start_date",
        "product_list",
        "days_since_start_date_to_premium_ratio",
        "greatest_n_rows_stddev_premium",
        F.array_join(F.array_sort(F.col("product_list")), ",").alias("product_list_text"),
        F.abs(F.col("n_rows") - 6).alias("n_rows_distance"),
    )
)

qualified = (
    annotated
    .crossJoin(annotated.agg(F.max("n_rows").alias("overall_max_n_rows")))
    .filter(F.col("n_rows") < F.col("overall_max_n_rows"))
    .drop("overall_max_n_rows")
)

screened = (
    qualified
    .crossJoin(qualified.agg(F.avg("min_days_since_start_date").alias("overall_avg_min_days_since_start_date")))
    .filter(F.col("min_days_since_start_date") > F.col("overall_avg_min_days_since_start_date"))
    .drop("overall_avg_min_days_since_start_date")
)

result = (
    screened
    .select(
        "product",
        "min_days_since_start_date",
        "days_since_start_date_to_premium_ratio",
        "product_list_text",
        "n_rows_distance",
        F.size(F.col("product_list")).alias("product_list_count"),
    )
    .orderBy(F.col("n_rows_distance").desc_nulls_first(), F.col("product").desc())
)
