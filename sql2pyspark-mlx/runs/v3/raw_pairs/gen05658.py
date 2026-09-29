from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("customer_id")
    .orderBy(F.col("customer_id").desc())
)
