from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(180, 400) & F.col("order_id").isin([2, 26, 32]))
    .select(
        "quantity",
        "product_id",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_scaled"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)
