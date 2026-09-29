from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
        F.round("quantity", 0).alias("quantity_rounded"),
    )
    .orderBy("quantity_rounded")
)
