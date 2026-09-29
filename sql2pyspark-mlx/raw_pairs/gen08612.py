from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") < 6)
    .select("unit_price", "order_id", "product_id")
)
