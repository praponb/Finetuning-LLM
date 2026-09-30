from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 17, 34]))
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.floor("quantity").alias("quantity_whole"),
    )
    .orderBy(F.col("product_id").desc())
)
