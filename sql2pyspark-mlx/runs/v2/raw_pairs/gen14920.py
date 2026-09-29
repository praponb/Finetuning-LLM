from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 160) & (F.col("end_date") < "2023-12-01"))
)
