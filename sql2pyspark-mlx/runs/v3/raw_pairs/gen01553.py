from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 150)
    .select(
        "product_id",
        "quantity",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)
