from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .select("end_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date").between("2023-12-01", "2025-09-01"))
        .select("end_date")
    )
    .distinct()
)
