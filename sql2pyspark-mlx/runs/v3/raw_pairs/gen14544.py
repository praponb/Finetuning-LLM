from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select(F.col("monthly_fee").alias("value"))
    .unionByName(df_products.select(F.col("cost").alias("value")))
)
