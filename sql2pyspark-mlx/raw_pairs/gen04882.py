from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").isNotNull())
    .select("order_date")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date").isNotNull())
        .select("end_date")
    )
)
