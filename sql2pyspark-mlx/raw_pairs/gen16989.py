from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 340)
    .select(
        "id",
        "quantity",
        "product_id",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)
