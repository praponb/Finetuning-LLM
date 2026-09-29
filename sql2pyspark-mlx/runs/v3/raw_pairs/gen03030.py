from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([23, 35]))
    .groupBy("order_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
    .filter(F.col("avg_quantity") >= 8.1)
    .select("order_id", "avg_quantity")
    .orderBy(F.col("order_id").desc())
    .limit(1)
)
