from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([3, 4, 10]))
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .orderBy("product_id")
    .limit(10)
)
