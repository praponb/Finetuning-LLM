from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status", "customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("status") != "CANCELLED")
        .select("status", "customer_id")
    )
)
