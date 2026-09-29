from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .select("id", "plan")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") == "enterprise")
        .select("id", "plan")
    )
    .distinct()
)
