from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id").isin([3, 5, 26, 32]) & (F.col("order_id") == 32)) | F.col("product_id").isin([15, 19, 31]))
    .select("unit_price")
)
