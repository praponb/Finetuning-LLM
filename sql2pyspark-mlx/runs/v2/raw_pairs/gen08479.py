from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "order_id",
        "unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_scaled"),
    )
    .orderBy("line_total", "unit_price")
)
