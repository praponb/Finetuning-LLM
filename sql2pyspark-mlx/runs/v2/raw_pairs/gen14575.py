from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("unit_price").between(150, 180))
    .withColumn("quantity_level", F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 2, "Medium").otherwise("Low"))
)

order_items_stats = (
    valid_order_items
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    order_items_stats
    .filter(F.col("total_quantity") < 32)
    .select("product_id", "total_quantity")
    .orderBy("total_quantity", "product_id")
    .limit(5)
)
