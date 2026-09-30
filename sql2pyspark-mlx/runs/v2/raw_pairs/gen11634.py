from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "CANCELLED", "ACTIVE"]))
    .select("status", "start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isin([11, 19]))
        .select("status", "start_date")
    )
)
