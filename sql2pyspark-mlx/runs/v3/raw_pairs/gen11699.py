from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 180)
    .select("product_id", (F.col("unit_price") * F.col("quantity")).alias("line_total"))
    .orderBy("product_id")
)
