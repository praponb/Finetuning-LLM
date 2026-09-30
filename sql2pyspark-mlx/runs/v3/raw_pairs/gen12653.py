from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "premium"]))
    .select("start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") >= "2024-01-01")
        .select("start_date")
    )
    .distinct()
)
