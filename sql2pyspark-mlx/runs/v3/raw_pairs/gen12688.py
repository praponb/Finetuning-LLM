from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([2, 21, 26, 31]))
    .select("unit_price")
    .subtract(df_subscriptions.select("monthly_fee"))
    .orderBy("unit_price")
)
