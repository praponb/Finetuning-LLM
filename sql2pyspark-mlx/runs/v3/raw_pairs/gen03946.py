from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 2) & (F.col("unit_price") <= 300))
    .select("id", "order_id", "product_id", "quantity")
    .orderBy("id")
    .limit(5)
)
