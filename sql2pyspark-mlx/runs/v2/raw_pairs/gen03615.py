from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "end_date", "customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("status").isin(["CANCELLED", "EXPIRED", "PAUSED"]))
        .select("id", "end_date", "customer_id")
    )
    .orderBy(F.col("id").desc())
)
