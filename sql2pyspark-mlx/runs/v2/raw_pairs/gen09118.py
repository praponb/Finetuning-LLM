from pyspark.sql import functions as F

valid_order_items = df_order_items.filter(F.col("product_id").isin([4, 12, 28]))

agg = (
    valid_order_items
    .groupBy("order_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    agg
    .select("order_id", "revenue")
    .orderBy(F.col("order_id").desc())
)
