from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-08-10").cast("date")) & F.col("product_id").isin([6, 7, 8]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    filtered
    .rollup(
        F.date_trunc("MONTH", "sale_date").alias("activity_month"),
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
    )
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("units_bit_or")
)
