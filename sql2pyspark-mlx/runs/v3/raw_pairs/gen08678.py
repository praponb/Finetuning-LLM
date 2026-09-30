from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("status")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date") <= "2023-03-01")
        .select("status")
    )
)
