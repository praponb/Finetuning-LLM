from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", "plan")
    .agg(F.countDistinct("monthly_fee").alias("unique_plans"))
    .orderBy("plan")
)
