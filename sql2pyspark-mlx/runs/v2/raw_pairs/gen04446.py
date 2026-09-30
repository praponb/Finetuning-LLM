from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") > "2023-04-01") & F.col("customer_id").isin([3, 28]))
    .select("customer_id")
)
