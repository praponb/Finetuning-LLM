from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(7, 8))
    .groupBy("order_id", "product_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("revenue") < 72)
    .orderBy(F.col("quantity_range").desc(), "order_id")
)
