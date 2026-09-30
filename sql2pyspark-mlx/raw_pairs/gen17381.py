from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .union(df_orders.filter(F.col("order_date") >= "2025-01-01").select("amount"))
    .distinct()
    .orderBy("unit_price")
)
