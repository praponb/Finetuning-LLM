from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([19, 27, 35, 37]))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total_2"),
    )
)
