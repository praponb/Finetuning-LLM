from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("unit_price").between(110, 350) | F.col("unit_price").isNotNull())
)

agg = (
    valid_order_items
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
)

result = agg.orderBy(F.col("unique_orders").desc())
