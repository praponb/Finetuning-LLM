from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-03-01", "2025-02-12") & F.col("customer_id").isNull())
    .select("end_date", "start_date")
)
