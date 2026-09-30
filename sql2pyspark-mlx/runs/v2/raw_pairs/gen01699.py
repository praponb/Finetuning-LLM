from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 12) & ((F.col("end_date") < "2025-10-09") | (F.col("end_date") > "2025-10-01")))
    .select("id", "plan")
)
