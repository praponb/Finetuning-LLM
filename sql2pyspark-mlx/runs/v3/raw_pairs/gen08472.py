from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .union(df_inventory.select("last_restock"))
    .orderBy("order_date")
)
