from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .unionByName(df_shipments.select(F.col("cost").alias("monthly_fee")))
    .distinct()
)
