from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([7, 25]))
    .select("end_date", "status")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date") <= "2025-10-01")
        .select("end_date", "status")
    )
)
