from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 350)
    .select("id", "product_id", "quantity", "order_id")
)
