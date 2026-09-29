from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "CANCELLED")
    .select("customer_id", "end_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date") >= "2023-12-18")
        .select("customer_id", "end_date")
    )
    .distinct()
)
