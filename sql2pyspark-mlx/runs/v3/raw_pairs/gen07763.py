from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("quantity") != 8) & (F.col("unit_price") > 120)) | F.col("order_id").isin([15, 16, 22, 23]))
    .select("id", "product_id", F.col("unit_price").cast("int").alias("unit_price_int"))
)
