from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([4, 31]))
    .select("plan", "end_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id") == 9)
        .select("plan", "end_date")
    )
)
