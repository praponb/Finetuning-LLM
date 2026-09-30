from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([15, 23]) & (F.col("order_id") == 4) & F.col("order_id").isin([7, 8, 21]))
    .groupBy("order_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.min("unit_price").alias("min_unit_price"),
    )
)
