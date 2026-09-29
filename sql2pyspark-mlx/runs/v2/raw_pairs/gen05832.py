from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 230)
    .groupBy("status")
    .pivot("plan", ["basic", "standard"])
    .agg(F.max("monthly_fee"))
)
