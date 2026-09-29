from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 28)
    .select(F.lit("order").alias("record_type"), "status")
    .unionByName(
        df_subscriptions
        .select(F.lit("subscription").alias("record_type"), "status")
    )
)
