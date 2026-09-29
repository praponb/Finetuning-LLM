from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date", F.lit("order").alias("origin"))
    .union(
        df_subscriptions
        .filter(F.col("customer_id").isin([35, 36]))
        .select("end_date", F.lit("subscription").alias("origin"))
    )
    .distinct()
    .orderBy(F.col("origin").desc())
)
