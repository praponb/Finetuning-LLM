from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .union(
        df_subscriptions
        .filter(F.month("start_date").isin([1, 8, 9]))
        .select("end_date")
    )
    .distinct()
)
