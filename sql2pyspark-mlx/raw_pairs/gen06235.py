from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("customer_id") == 28)
    .withColumn("status_lower", F.lower("status"))
)

agg = (
    base
    .groupBy("plan")
    .agg(F.min("end_date").alias("first_end_date"))
)

result = (
    agg
    .select("plan", "first_end_date")
    .orderBy("first_end_date")
)
