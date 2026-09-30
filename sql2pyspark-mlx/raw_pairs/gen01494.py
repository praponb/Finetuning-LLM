from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() | F.col("quantity").isNotNull())
    .groupBy("order_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") > 7.0)
    .drop("min_quantity")
    .orderBy("quantity_range")
)
