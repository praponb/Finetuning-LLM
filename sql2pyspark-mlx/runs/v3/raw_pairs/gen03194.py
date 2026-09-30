from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([7, 17, 40]) & F.col("unit_price").between(200, 230) & F.col("unit_price").between(130, 180))
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .orderBy(F.col("product_id").desc())
    .limit(1)
)
