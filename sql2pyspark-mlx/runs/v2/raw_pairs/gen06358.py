from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.countDistinct("monthly_fee").alias("unique_statuss"))
)
