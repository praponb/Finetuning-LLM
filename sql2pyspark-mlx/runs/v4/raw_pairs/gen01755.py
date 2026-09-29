from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "customer_id", "plan")
    .unionByName(
        df_subscriptions
        .filter(F.col("status") == "EXPIRED")
        .select("id", "customer_id", "plan")
    )
    .distinct()
    .orderBy(F.col("plan").desc())
)
