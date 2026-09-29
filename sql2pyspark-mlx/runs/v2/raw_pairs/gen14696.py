from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "premium", "basic"]))
    .select("id", "end_date", "start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isin([5, 21, 34]))
        .select("id", "end_date", "start_date")
    )
    .orderBy("id")
)
