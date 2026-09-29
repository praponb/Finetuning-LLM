from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee", F.lit("subscription").alias("source"))
    .unionByName(
        df_order_items
        .select(
        F.col("unit_price").alias("monthly_fee"),
        F.lit("order_item").alias("source"),
    )
    )
)
