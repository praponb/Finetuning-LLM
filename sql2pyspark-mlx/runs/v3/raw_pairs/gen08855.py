from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy(F.col("order_id").desc(), F.col("avg_quantity").desc())
    .limit(10)
)
