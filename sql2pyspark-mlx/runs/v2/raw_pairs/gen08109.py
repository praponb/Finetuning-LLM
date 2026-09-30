from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("unit_price") <= 350) & (F.col("order_id") == 21)) | F.col("quantity").between(4, 8))
    .select("unit_price")
)
