from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total_2"),
    )
    .orderBy("id")
)
