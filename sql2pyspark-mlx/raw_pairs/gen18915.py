from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 8)
    .select(
        "id",
        "product_id",
        "order_id",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)
