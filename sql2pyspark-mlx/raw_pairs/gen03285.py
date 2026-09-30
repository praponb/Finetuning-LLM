from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "CANCELLED")
    .select("amount")
    .union(df_order_items.filter(F.col("quantity") > 4).select("unit_price"))
    .orderBy("amount")
)
