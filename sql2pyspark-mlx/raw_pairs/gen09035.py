from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("customer_id", "id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id", "id")
    )
)
