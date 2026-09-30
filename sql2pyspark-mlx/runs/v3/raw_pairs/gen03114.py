from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .select("plan", "customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date") <= "2023-03-16")
        .select("plan", "customer_id")
    )
)
