from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 110) & F.col("quantity").between(5, 7))
    .select(
        "unit_price",
        "order_id",
        "product_id",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)
