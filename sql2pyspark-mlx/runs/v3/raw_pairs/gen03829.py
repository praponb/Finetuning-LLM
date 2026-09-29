from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 5)
    .groupBy("order_id", "product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.sum("quantity").alias("total_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
    .filter(F.col("revenue") < 55)
    .drop("revenue")
    .orderBy(F.col("unit_price_range").desc(), F.col("order_id").desc())
)
