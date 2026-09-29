from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2024-05-01")
    .select("end_date", "start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") == "enterprise")
        .select("end_date", "start_date")
    )
    .orderBy("start_date")
)
