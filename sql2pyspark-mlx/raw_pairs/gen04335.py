from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") >= 3) & F.col("unit_price").between(240, 310))
    .select(
        "id",
        "unit_price",
        "product_id",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
    .orderBy("line_total", "unit_price", "id")
    .limit(5)
)
