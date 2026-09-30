from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount")
    .subtract(df_order_items.select("unit_price"))
    .orderBy("amount")
)
