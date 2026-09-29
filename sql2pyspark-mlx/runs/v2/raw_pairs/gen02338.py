from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") >= "2023-12-01") | (F.col("monthly_fee") >= 240))
    .select("id", "monthly_fee", F.substring("status", 1, 4).alias("status_prefix"))
)
