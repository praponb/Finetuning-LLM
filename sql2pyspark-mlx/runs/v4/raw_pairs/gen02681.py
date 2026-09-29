from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("order_id") == 24) & F.col("product_id").isNotNull()) | (F.col("quantity") <= 4))
    .select(
        "id",
        "product_id",
        "order_id",
        F.floor("unit_price").alias("unit_price_whole"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
