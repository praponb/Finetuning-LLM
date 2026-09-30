from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2025-10-01")
    .select(F.col("monthly_fee").alias("value"))
    .intersect(df_orders.select(F.col("amount").alias("value")))
    .orderBy("value")
)
