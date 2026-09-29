from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") > "2023-12-27") | (F.col("plan") == "premium"))
    .select("id", "customer_id")
)
