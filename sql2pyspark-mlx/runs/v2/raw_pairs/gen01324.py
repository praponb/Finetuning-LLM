from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") <= "2024-11-13")
    .select("id", "plan", "start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("status") == "PAUSED")
        .select("id", "plan", "start_date")
    )
)
