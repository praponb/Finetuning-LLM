from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 21, 30, 36]))
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)
