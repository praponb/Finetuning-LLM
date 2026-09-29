from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.max("quantity").alias("max_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
    .filter(F.col("revenue") > 55)
    .drop("revenue")
)
