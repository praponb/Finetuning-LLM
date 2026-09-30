from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.year("start_date") == 2024) & (F.col("start_date") < "2025-07-11"))
    .withColumn("start_date_minus_43d", F.date_sub("start_date", 43))
)
