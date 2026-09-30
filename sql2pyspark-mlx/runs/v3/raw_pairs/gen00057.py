from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 280)
    .select("order_id", (F.col("unit_price") - F.col("quantity")).alias("line_total"))
)
