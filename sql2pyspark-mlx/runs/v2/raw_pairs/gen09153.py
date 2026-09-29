from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(((F.col("end_date") > "2025-08-01") & (F.col("start_date") < "2024-04-01")) | F.col("start_date").between("2023-04-07", "2024-08-01"))
    .select("id", "monthly_fee", "status")
)
