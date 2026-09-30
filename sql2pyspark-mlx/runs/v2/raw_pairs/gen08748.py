from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 120) & F.col("product_id").isin([13, 30, 31, 33]) & (F.col("quantity") <= 5))
    .select("id", "order_id")
    .orderBy("id")
)
