from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 4)
    .select("unit_price")
    .union(df_subscriptions.select("monthly_fee"))
)
