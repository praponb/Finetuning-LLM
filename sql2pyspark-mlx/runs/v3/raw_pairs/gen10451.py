from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("status")
    )
)
