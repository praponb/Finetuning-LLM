from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2024-04-01")
    .select("id", "customer_id", "end_date")
    .subtract(
        df_subscriptions
        .filter(F.month("start_date").isin([2, 5, 9]))
        .select("id", "customer_id", "end_date")
    )
)
