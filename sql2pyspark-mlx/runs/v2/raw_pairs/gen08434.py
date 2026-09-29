from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([9, 15, 20, 28]))
    .groupBy("product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .filter(F.col("unit_price_count") > 1)
    .orderBy(F.col("unit_price_count").desc())
)
