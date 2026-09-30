from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(30, 34))
    .select(F.lit("order").alias("kind"), "status")
    .unionByName(df_subscriptions.select(F.lit("subscription").alias("kind"), "status"))
    .distinct()
    .orderBy("status")
)
