from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
    .orderBy("plan")
)
