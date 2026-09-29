from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-11-01", "2024-08-01"))
    .select("start_date", "monthly_fee", "end_date")
)
