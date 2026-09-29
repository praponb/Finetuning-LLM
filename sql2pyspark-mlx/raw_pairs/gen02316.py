from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 6)
    .groupBy("order_id")
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") > 3.8)
    .drop("max_quantity")
)
