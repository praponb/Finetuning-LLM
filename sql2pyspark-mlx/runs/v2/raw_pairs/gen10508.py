from pyspark.sql import functions as F

base = (
    df_order_items
    .filter((F.col("unit_price") < 230) | F.col("order_id").isin([6, 17, 29, 36]))
)

summary = (
    base
    .groupBy("order_id", "product_id")
    .agg(F.countDistinct("unit_price").alias("unique_products"))
)

result = (
    summary
    .select("order_id", "product_id", "unique_products")
    .orderBy(F.col("product_id").desc(), "order_id")
    .limit(10)
)
