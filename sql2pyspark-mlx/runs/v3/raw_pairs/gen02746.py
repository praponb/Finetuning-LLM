from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("revenue").alias("value"), F.lit("store_sale").alias("source"))
    .unionByName(
        df_subscriptions
        .select(F.col("monthly_fee").alias("value"), F.lit("subscription").alias("source"))
    )
)
