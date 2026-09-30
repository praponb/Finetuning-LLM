from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .intersect(df_orders.select("order_date"))
    .orderBy("last_restock")
)
