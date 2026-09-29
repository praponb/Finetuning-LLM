from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~(F.month("start_date").isin([1, 4, 6]) & (F.col("end_date") < "2025-06-01")))
    .select("id", "end_date", "start_date", "customer_id")
)
