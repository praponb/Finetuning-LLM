from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price").between(250, 260) & (F.col("order_id") == 31)) | (F.col("order_id") == 19))
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy(F.col("min_quantity").desc(), F.col("total_quantity").desc())
)
