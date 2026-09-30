from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") < 7)
    .groupBy("product_id", "order_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("cnt"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("min_unit_price") >= 370)
    .drop("min_unit_price")
    .orderBy("avg_quantity")
)
