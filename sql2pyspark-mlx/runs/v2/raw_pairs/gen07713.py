from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .select("customer_id", "end_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") == "premium")
        .select("customer_id", "end_date")
    )
    .distinct()
)
