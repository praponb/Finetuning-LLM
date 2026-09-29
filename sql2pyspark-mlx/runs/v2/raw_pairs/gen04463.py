from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("unit_price").alias("unit_price_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy(F.col("unit_price_count").desc(), "product_id")
    .limit(5)
)
