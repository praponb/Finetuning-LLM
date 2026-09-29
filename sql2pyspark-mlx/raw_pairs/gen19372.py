from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "PAUSED", "EXPIRED"]))
    .withColumn("monthly_fee_adjusted", F.round((F.col("monthly_fee") * 100), 2))
    .withColumn("clean_status", F.trim("status"))
    .orderBy("plan", "id")
    .limit(20)
)
