from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id", "product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .filter(F.col("max_unit_price") < 320)
    .orderBy("max_unit_price")
)
