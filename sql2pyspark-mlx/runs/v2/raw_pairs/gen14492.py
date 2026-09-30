from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("order_id") == 28) & (F.col("unit_price") <= 180)) | F.col("product_id").isin([32, 39]))
    .select("id", "unit_price")
    .orderBy(F.col("id").desc())
)
