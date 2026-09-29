from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") < 5.7)
    .orderBy(F.col("order_id").desc(), F.col("avg_quantity").desc())
)
