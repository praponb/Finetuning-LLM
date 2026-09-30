from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("units").between(36, 40) & (F.col("store_id").isin([2, 4, 5, 6]) | F.col("store_id").isin([1, 2, 4, 5])))
)

summary = (
    recent_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(F.countDistinct("revenue").alias("unique_products"))
)

result = (
    summary
    .filter(F.col("unique_products") < 8)
    .orderBy("sale_month", "unique_products")
)
