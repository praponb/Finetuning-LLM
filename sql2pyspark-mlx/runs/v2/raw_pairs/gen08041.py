from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(~((F.col("quantity") > 7) & (F.col("quantity") == 6)))

product_id_summary = (
    filtered_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    product_id_summary
    .filter(F.col("revenue") >= 23)
    .orderBy("unit_price_bucket", "product_id")
    .limit(1)
)
