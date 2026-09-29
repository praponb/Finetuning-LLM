from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)
