from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") <= "2024-10-01")
        .select("status")
    )
    .distinct()
)
