from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("order_id") == 13) & F.col("unit_price").between(240, 250)) | (F.col("quantity") != 4))
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)
