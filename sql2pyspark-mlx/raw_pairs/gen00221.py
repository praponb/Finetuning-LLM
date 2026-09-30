from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 7))
    .select(
        "id",
        "unit_price",
        "product_id",
        "order_id",
        F.round((F.col("unit_price") * 1.07), 2).alias("unit_price_scaled"),
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
    .orderBy(F.col("unit_price_scaled").desc(), "id")
    .limit(50)
)
