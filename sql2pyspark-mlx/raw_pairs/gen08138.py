from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([8, 34, 40]) & F.col("unit_price").between(150, 400) & (F.col("unit_price") <= 220))
    .select(
        "id",
        "unit_price",
        "quantity",
        "product_id",
        F.round("quantity", 2).alias("quantity_rounded"),
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)
