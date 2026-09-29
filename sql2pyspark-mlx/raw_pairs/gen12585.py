from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("revenue") <= 1000)
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.col("units").cast("string").alias("units_num"),
    )
)

result = (
    base
    .groupBy(
        "product_id",
        F.when(F.col("revenue") >= 3800, "LARGE").otherwise("SMALL").alias("revenue_tier"),
    )
    .agg(
        F.avg("units").alias("avg_units"),
        F.percentile_approx("revenue", 0.9).alias("revenue_p90"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("units"), 2).alias("avg_units_2"),
    )
    .orderBy(F.col("revenue_tier").desc())
)
