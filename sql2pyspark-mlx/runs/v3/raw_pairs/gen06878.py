from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 39)
    .select("id", "unit_price", "product_id")
)
