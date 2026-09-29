from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "basic") & (F.col("monthly_fee") < 140))
    .withColumn("clean_status", F.trim("status"))
)
