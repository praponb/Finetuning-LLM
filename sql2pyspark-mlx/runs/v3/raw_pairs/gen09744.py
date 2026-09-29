from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 29)
    .select("start_date", "end_date")
    .unionByName(
        df_subscriptions
        .filter(F.year("start_date") == 2025)
        .select("start_date", "end_date")
    )
    .orderBy("start_date")
)
