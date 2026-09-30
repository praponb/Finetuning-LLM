from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("quantity") <= 4) & (F.col("product_id") == 24)) | (F.col("quantity") < 8))
    .select("quantity", (F.col("unit_price") * F.col("quantity")).alias("line_total"))
)
