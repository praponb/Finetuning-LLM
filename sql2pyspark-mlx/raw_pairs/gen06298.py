from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.lit("subscription").alias("origin"), "monthly_fee")
    .union(df_orders.select(F.lit("order").alias("origin"), "amount"))
    .orderBy("origin")
)
