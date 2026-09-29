from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 6, 7, 8]))
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
)

store_sales_stats = (
    valid_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.countDistinct("units").alias("unique_stores"),
    )
)

result = (
    store_sales_stats
    .select("sale_cohort", "unique_stores")
    .orderBy(F.col("unique_stores").desc(), F.col("sale_cohort").desc())
)
