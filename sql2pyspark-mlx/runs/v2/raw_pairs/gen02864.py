from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        "order_id",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
    .orderBy(F.col("product_id").desc(), F.col("id").desc())
)
