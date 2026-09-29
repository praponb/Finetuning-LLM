from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("unit_price").isNotNull() & ((F.col("product_id") == 31) | (F.col("unit_price") < 360)))
)

agg = (
    base
    .groupBy("order_id")
    .agg(
        F.count("*").alias("order_item_count"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = agg.select("order_id", "order_item_count", "quantity_count")
