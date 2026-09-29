from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNull())
    .select("status")
    .subtract(df_subscriptions.select("status"))
    .orderBy("status")
)
