from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 36) & F.col("product_id").isin([16, 28, 33]))
    .groupBy("order_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.min("unit_price").alias("min_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .filter(F.col("unit_price_count") < 3)
    .drop("unit_price_count")
)
