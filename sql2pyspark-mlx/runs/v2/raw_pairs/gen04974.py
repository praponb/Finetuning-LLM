from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 8)
    .select("id", "order_id", "unit_price")
)
