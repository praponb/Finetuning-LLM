from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter((F.col("quantity") >= 7) & F.col("product_id").isNull())

summary = (
    filtered_order_items
    .groupBy("product_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    summary
    .filter(F.col("revenue") >= 77)
    .select("product_id", "revenue")
    .orderBy(F.col("product_id").desc(), "revenue")
    .limit(1)
)
