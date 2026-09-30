from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([7, 11, 19, 36]))
    .select("end_date", "start_date", "plan")
)
