from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([4, 6, 7]) & F.col("customer_id").isNotNull())
    .select("start_date", "monthly_fee", "plan")
)
