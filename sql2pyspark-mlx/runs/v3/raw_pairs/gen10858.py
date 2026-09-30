from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([13, 30]) & (F.col("status").isin(["CANCELLED", "ACTIVE"]) | (F.col("monthly_fee") >= 190)))
    .groupBy("plan")
    .agg(F.countDistinct("monthly_fee").alias("unique_statuss"))
)
