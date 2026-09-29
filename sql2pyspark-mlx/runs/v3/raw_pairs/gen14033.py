from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED", "CANCELLED"]))
    .select("plan", "start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date") >= "2023-10-06")
        .select("plan", "start_date")
    )
    .orderBy("start_date")
)
