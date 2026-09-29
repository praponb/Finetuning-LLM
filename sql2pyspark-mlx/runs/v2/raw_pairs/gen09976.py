from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 32)
    .select("id", "order_id", "product_id")
)
