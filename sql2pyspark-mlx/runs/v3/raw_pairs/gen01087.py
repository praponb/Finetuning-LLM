from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~((F.col("order_id") == 2) | (F.col("order_id") == 14)))
    .select("id", "unit_price", "quantity", "product_id")
)
