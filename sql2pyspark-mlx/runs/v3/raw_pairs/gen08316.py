from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .subtract(
        df_subscriptions
        .filter(F.month("end_date").isin([1, 6, 12]))
        .select("start_date")
    )
)
