from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price", "order_id")
    .orderBy("unit_price")
)
