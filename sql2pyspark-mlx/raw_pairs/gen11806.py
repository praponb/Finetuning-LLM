from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("product_id").isNull())

summary = (
    recent_order_items
    .groupBy("product_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    summary
    .filter(F.col("min_quantity") < 3.8)
    .orderBy("product_id")
)
