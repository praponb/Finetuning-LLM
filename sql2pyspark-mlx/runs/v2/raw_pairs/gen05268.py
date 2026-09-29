from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([3, 11, 36]))
    .groupBy("product_id", "order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy("order_id")
)
