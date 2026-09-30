from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2024-05-01")
    .orderBy(F.col("customer_id").desc())
)
