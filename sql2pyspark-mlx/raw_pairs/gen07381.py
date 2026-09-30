from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount", F.lit("order").alias("record_type"))
    .union(
        df_subscriptions
        .select("monthly_fee", F.lit("subscription").alias("record_type"))
    )
    .distinct()
)
