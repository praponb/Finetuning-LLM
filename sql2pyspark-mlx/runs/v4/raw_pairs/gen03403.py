from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~(F.col("status").isin(["ACTIVE", "EXPIRED", "CANCELLED"]) & F.col("customer_id").isin([30, 40])))
    .select("plan")
)
