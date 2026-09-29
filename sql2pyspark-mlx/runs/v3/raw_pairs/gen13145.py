from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") >= 34)
    .select("product_id", "order_id", "quantity_count", "avg_unit_price")
    .orderBy(F.col("order_id").desc())
)
