from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([10, 30]) & (F.col("quantity") == 7) & F.col("product_id").isNull())
    .select(
        "id",
        "unit_price",
        "quantity",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
    .orderBy("id")
)
