from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-07-01", "2024-11-09") & (F.col("start_date") <= "2024-08-06") & F.col("end_date").between("2024-11-18", "2025-02-01"))
    .select("id", "start_date")
)
