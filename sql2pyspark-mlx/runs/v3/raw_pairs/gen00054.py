from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .subtract(df_order_items.select("unit_price"))
    .orderBy(F.col("monthly_fee").desc())
)
