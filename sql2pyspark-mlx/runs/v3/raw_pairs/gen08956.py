from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-12-01", "2025-01-13") & F.col("monthly_fee").between(160, 240) & (F.col("plan") == "basic"))
    .select("id", "start_date", "status", "plan")
)
