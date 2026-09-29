from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 100) & ((F.col("start_date") < "2023-11-01") | (F.col("monthly_fee") >= 80)))
    .select("id", "plan", F.floor("monthly_fee").alias("monthly_fee_whole"))
)
