from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 37)
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .orderBy("status")
)
