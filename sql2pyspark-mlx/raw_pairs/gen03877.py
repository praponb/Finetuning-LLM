from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 25)
    .select("order_id", "unit_price")
)
