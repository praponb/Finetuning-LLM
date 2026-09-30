from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.22, 0.29))
    .select("order_date", F.lit("order").alias("source"))
    .union(
        df_subscriptions
        .select("start_date", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy("order_date")
)
