from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .filter(F.col("unique_products") > 7)
    .orderBy("order_id")
    .limit(3)
)
