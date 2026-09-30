from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("start_date") > "2023-09-01"))
    .select("id", "start_date", "status", "end_date")
    .orderBy(F.col("status").desc())
)
