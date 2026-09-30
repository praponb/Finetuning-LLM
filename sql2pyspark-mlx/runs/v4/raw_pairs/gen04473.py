from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("customer_id", "status")
    .intersect(
        df_subscriptions
        .filter(F.col("status") == "ACTIVE")
        .select("customer_id", "status")
    )
)
