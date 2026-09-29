from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("plan", "status")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id") == 38)
        .select("plan", "status")
    )
)
