from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter(F.col("product_id").isNull() & (F.col("quantity") <= 6) & F.col("product_id").isin([12, 19, 30, 37]))
)

order_id_summary = (
    filtered_order_items
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    order_id_summary
    .select("order_id", "quantity_bucket", "min_quantity")
    .orderBy(F.col("quantity_bucket").desc())
)
