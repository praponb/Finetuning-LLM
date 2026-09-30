from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "product_id", "unit_price", "quantity")
    .orderBy("id", "unit_price")
)
