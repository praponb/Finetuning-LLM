from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("unit_price") >= 180) & F.col("order_id").isin([5, 40])) | F.col("product_id").isin([25, 26, 32]))
    .select("product_id", "unit_price", "order_id")
    .orderBy(F.col("product_id").desc())
)
