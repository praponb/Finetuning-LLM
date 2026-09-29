from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.24)
    .select("amount")
    .union(df_order_items.filter(F.col("unit_price") >= 200).select("unit_price"))
)
