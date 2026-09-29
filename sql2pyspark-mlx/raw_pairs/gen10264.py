from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .select(F.col("amount").alias("value"))
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select(F.col("monthly_fee").alias("value"))
    )
)
