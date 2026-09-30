from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
    .filter(F.col("unit_price_count") >= 4)
    .select("order_id", "unit_price_count")
    .orderBy(F.col("order_id").desc())
    .limit(20)
)
