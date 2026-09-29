from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "product_id",
        "unit_price",
        "quantity",
        F.round((F.col("unit_price") * 1.15), 2).alias("unit_price_scaled"),
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
    .orderBy(F.col("unit_price").desc())
)
