from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") <= "2025-05-13")
        .select("status")
    )
)
