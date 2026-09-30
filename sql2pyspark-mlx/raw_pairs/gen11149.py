from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status", "customer_id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("start_date") > "2024-05-01")
        .select("status", "customer_id")
    )
)
