from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & F.col("unit_price").between(160, 360) & F.col("quantity").between(4, 5))
    .select(
        "id",
        "quantity",
        "order_id",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)
