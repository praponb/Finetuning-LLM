from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 4]) & F.month("sale_date").isin([6, 8, 10]))
)

store_sales_stats = (
    base
    .groupBy("product_id")
    .agg(
        F.countDistinct("units").alias("unique_stores"),
        F.countDistinct("revenue").alias("unique_products"),
    )
)

result = (
    store_sales_stats
    .orderBy(F.col("unique_products").desc(), "product_id")
    .limit(10)
)
