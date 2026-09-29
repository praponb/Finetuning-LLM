from pyspark.sql import functions as F

min_start_date = df_subscriptions.agg(F.min("start_date")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("start_date") == min_start_date)
    .select("id", "start_date", "customer_id")
    .orderBy("id", F.col("customer_id").desc())
)
