from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 2)
    .select(
        "id",
        "quantity",
        "unit_price",
        "product_id",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
    )
)
