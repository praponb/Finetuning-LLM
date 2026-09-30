from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") <= 0.4)
    .select("id", "customer_id", "channel")
    .orderBy("customer_id", F.col("channel").desc())
)
