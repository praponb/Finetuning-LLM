from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") > "2024-01-01")
    .select("id", "end_date", "start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") > "2023-06-30")
        .select("id", "end_date", "start_date")
    )
)
