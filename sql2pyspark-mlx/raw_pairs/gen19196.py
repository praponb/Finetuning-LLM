from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "quantity", "product_id", "unit_price")
    .orderBy("unit_price", "id")
    .limit(10)
)
