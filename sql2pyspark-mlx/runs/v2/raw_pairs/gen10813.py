from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("min_unit_price") > 190)
    .drop("min_unit_price")
)
