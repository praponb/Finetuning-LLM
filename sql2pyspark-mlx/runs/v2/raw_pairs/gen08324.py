from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .select("status", "customer_id")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isin([15, 21, 34]))
        .select("status", "customer_id")
    )
)
