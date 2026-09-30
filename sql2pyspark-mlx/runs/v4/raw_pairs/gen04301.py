from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
    .orderBy(F.col("quantity").desc(), F.col("line_total").desc())
)
