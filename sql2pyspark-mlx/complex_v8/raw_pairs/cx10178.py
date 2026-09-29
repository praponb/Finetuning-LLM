from pyspark.sql import functions as F

base = (
    df_policies
    .filter((F.col("status") == "EXPIRED") & (F.col("start_date") < F.lit("2025-02-13").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

labeled = (
    base
    .select(
        "*",
        F.trunc(F.col("end_date"), "MM").alias("end_date_month_start"),
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
        F.when(F.col("premium") >= 3544.1, "A").when(F.col("premium") >= 2289.0, "B").otherwise("C").alias("premium_tier"),
    )
)

stats = (
    labeled
    .groupBy("holder_id", F.year("start_date").alias("start_date_year"))
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
        F.min("end_date").alias("first_end_date"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
)

without_matches = (
    stats
    .crossJoin(stats.agg(F.max("start_date_year").alias("overall_max_start_date_year")))
    .filter(F.col("start_date_year") < F.col("overall_max_start_date_year"))
    .drop("overall_max_start_date_year")
)

result = (
    without_matches
    .select(
        "holder_id",
        "start_date_year",
        "stddev_premium",
        "first_end_date",
        "product_list",
        "status_list",
        F.coalesce(F.col("start_date_year"), F.lit(0)).alias("start_date_year_filled"),
    )
    .orderBy(
        F.col("stddev_premium").desc(),
        F.col("start_date_year").asc_nulls_last(),
        "holder_id",
    )
    .limit(5)
)
