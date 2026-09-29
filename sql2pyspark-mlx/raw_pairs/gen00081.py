from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("quantity") > 8) & F.col("order_id").isin([17, 26, 32])) | F.col("product_id").isin([11, 13, 28, 33]))
    .select("unit_price")
    .orderBy(F.col("unit_price").desc())
)
