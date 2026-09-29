from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .select("id", "end_date")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date") < "2025-08-11")
        .select("id", "end_date")
    )
)
