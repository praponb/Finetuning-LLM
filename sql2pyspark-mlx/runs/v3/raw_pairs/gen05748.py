from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date", F.lit("order").alias("source"))
    .union(
        df_subscriptions
        .select("start_date", F.lit("subscription").alias("source"))
    )
)
