from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .filter(F.col("total_monthly_fee") > 910)
)
