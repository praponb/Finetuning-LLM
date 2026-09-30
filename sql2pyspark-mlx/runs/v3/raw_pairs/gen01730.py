from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "premium") | F.col("customer_id").isNotNull())
    .select("id", "status", "customer_id", F.upper("status").alias("status_upper"))
)
