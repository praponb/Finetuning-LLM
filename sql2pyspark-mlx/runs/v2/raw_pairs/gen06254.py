from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "ACTIVE") & F.month("end_date").isin([3, 6, 8]) & (F.col("status") == "EXPIRED"))
    .select("id", "monthly_fee", "end_date", "start_date")
    .orderBy(F.col("monthly_fee").desc())
)
