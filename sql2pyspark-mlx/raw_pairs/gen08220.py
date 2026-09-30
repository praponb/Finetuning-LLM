from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("plan")
    .unionByName(df_customers.select(F.col("country").alias("plan")))
)
