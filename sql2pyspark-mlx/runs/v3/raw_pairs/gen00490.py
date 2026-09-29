from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .select("customer_id", "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") == "enterprise")
        .select("customer_id", "status")
    )
    .orderBy(F.col("customer_id").desc())
)
