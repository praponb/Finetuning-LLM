from pyspark.sql import functions as F

base = df_order_items.filter(F.col("order_id").isin([10, 12, 25]))

agg = (
    base
    .groupBy("product_id", "order_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.count("*").alias("order_item_count"),
    )
)

result = (
    agg
    .select("product_id", "order_id", "unique_products", "order_item_count")
    .orderBy(F.col("product_id").desc())
)
