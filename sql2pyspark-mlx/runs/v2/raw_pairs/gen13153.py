from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") != "paypal")
    .select("status")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("status")
    )
)
