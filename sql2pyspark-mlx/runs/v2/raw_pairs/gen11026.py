from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([11, 38]))
    .select("id", "start_date")
)
