from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~((F.col("plan") == "standard") & (F.col("plan") == "basic")))
    .withColumn("status_length", F.length("status"))
)
