from pyspark.sql import functions as F

src = df_order_items.filter(F.col("product_id").isNull())

order_items_stats = (
    src
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
)

result = order_items_stats
