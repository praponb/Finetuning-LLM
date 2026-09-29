from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(130, 340) | F.col("product_id").isin([9, 22, 34]))
    .select(
        "order_id",
        "quantity",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total_2"),
    )
    .orderBy("line_total")
)
