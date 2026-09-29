from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 120)
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy("avg_quantity")
)
