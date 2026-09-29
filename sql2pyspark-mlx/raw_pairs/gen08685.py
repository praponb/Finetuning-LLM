from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 300) & (F.col("quantity") > 4) & (F.col("product_id") == 32))
    .groupBy("order_id", "product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") < 2)
    .orderBy("product_id", "order_id")
    .limit(10)
)
