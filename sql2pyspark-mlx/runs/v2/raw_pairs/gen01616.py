from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 210)
    .agg(F.count("quantity").alias("quantity_count"))
    .orderBy(F.col("quantity_count").desc())
)
