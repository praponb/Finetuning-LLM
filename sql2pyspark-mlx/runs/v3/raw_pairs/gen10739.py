from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 170) & F.col("status").isin(["PAUSED", "CANCELLED"]))
    .groupBy("plan")
    .agg(F.max("monthly_fee").alias("max_monthly_fee"))
    .filter(F.col("max_monthly_fee") < 170)
)
