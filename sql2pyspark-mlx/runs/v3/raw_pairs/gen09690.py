from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id").isin([10, 17, 18]))
        .select("id", "customer_id")
    )
)
