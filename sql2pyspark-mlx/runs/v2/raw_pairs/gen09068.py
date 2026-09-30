from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-11-01", "2025-08-01"))
    .select("id", "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee").between(170, 200))
        .select("id", "status")
    )
)
