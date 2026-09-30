from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "standard"]))
    .select("id", "start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date") > "2024-01-26")
        .select("id", "start_date")
    )
    .distinct()
    .orderBy("id")
)
