from pyspark.sql import functions as F

result = (
    df_order_items
    .select("order_id", "unit_price", "product_id")
    .orderBy("order_id")
)
