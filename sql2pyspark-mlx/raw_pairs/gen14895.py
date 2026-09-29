from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([10, 19, 33, 39]) & (F.col("plan") == "premium"))
    .select("start_date", "customer_id")
)
