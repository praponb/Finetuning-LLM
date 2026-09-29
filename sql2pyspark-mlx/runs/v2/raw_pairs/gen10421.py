from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "CLOSED")
    .select("status")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date").between("2024-08-01", "2025-10-17"))
        .select("status")
    )
)
