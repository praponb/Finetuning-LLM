from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("quantity_count") > 3)
    .select("product_id", "order_id", "quantity_count", "unique_products")
    .orderBy("quantity_count", F.col("product_id").desc(), "order_id")
    .limit(5)
)
