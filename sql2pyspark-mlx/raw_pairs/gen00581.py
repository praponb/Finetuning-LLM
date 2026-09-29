from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(((F.col("monthly_fee") < 140) & (F.col("status") != "ACTIVE")) | F.col("plan").isin(["enterprise", "premium", "basic"]))
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
)
