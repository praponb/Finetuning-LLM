from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull() | F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        F.ceil("unit_price").alias("unit_price_whole"),
        F.round((F.col("unit_price") * 1.1), 2).alias("adjusted_unit_price"),
    )
    .orderBy(F.col("unit_price_whole").desc())
)
