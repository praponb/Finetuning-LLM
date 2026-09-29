from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.count("quantity").alias("quantity_count"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .filter(F.col("revenue") >= 55)
    .orderBy(F.col("stddev_unit_price").desc(), "quantity_count", "product_id")
    .limit(5)
)
