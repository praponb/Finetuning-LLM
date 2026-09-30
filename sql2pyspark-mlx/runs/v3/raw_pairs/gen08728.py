from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
    .filter(F.col("avg_quantity") < 7.7)
    .orderBy(F.col("order_id").desc())
)
