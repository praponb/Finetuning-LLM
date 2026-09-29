from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.countDistinct("monthly_fee").alias("unique_plans"))
)
