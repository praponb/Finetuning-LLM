from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") < "2024-03-01") & ((F.col("plan") == "enterprise") | (F.col("status") == "PAUSED")))
    .withColumn("status_length", F.length("status"))
)
