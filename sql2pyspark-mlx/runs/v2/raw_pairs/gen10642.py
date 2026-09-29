from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter((F.col("product_id") == 32) & (F.col("product_id").isin([4, 9, 14, 18]) | F.col("quantity").isNotNull()))
)

summary = (
    recent_order_items
    .groupBy("product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    summary
    .select("product_id", "total_unit_price")
    .orderBy("total_unit_price", "product_id")
    .limit(5)
)
