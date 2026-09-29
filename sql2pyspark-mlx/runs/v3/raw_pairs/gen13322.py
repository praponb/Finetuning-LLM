from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "ACTIVE", "EXPIRED"]))
    .select("id", "status")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date") <= "2023-06-13")
        .select("id", "status")
    )
)
