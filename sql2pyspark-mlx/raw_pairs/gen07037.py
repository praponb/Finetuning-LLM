from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-09-11").cast("date"))
    .withColumn("revenue_per_unit", F.round((F.col("units") / F.col("revenue")), 2))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("revenue") >= 4000, "LARGE").otherwise("SMALL").alias("revenue_tier"),
    )
    .agg(
        F.percentile_approx("revenue_per_unit", 0.5).alias("revenue_per_unit_p50"),
        F.min("revenue").alias("min_revenue"),
    )
    .filter(F.col("min_revenue") > 0)
    .orderBy(F.col("revenue_per_unit_p50").desc_nulls_first())
)
