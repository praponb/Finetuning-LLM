from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "EXPIRED"]) & F.col("start_date").between("2024-03-13", "2025-10-19") & F.col("status").isin(["ACTIVE", "PAUSED", "EXPIRED"]))
    .select("id", "end_date", "monthly_fee", "status")
    .orderBy("end_date")
)
