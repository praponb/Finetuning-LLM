from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1200, 2100) & F.col("product_id").isin([2, 6, 7, 8]) & F.col("store_id").isin([1, 3, 4]))
    .groupBy("product_id", "store_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.sum("revenue").alias("total_revenue"),
        F.min("sale_date").alias("first_sale_date"),
        F.countDistinct("units").alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 4)
    .drop("unique_products")
    .orderBy(F.col("store_id").desc())
)
