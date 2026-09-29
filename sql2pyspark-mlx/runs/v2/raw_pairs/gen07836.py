from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") <= 8) | F.col("order_id").isin([22, 25, 30, 36]))
    .groupBy("product_id", "order_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.min("unit_price").alias("min_unit_price"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") < 7.3)
    .drop("max_quantity")
    .orderBy("revenue")
)
