from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1260, 1500))
    .select("status")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date") <= "2025-05-01")
        .select("status")
    )
)
