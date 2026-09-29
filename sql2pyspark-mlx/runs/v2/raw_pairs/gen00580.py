from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id").isin([12, 15, 31, 37]) & (F.col("unit_price") >= 370)) | F.col("quantity").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        F.ceil("unit_price").alias("unit_price_whole"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
    .orderBy(F.col("order_id").desc(), "id")
    .limit(10)
)
