from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2023-09-01")
    .orderBy("customer_id")
)
