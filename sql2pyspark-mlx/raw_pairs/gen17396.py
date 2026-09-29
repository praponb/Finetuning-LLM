from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([3, 15]) & ((F.col("unit_price") >= 280) | (F.col("unit_price") > 250)))
    .select("id", "product_id")
)
