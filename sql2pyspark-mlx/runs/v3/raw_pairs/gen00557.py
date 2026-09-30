from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([6, 14, 17]))
    .groupBy("product_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
    .filter(F.col("unit_price_count") >= 9)
    .select("product_id", "unit_price_count")
    .orderBy("product_id")
    .limit(3)
)
