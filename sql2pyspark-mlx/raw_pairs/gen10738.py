from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 340) & (F.col("quantity") <= 8))
    .groupBy("order_id")
    .agg(F.min("quantity").alias("min_quantity"))
    .orderBy("min_quantity")
)
