from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 120)
    .select(
        "unit_price",
        F.col("quantity").cast("string").alias("quantity_num"),
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)
