from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 30)
    .select("start_date")
    .subtract(
        df_orders
        .filter(F.month("order_date").isin([3, 8, 12]))
        .select("order_date")
    )
    .orderBy("start_date")
)
