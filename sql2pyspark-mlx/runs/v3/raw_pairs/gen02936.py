from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") <= 8) & (F.col("unit_price") >= 250))
    .agg(F.count("quantity").alias("quantity_count"))
)
