from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "premium")
    .select("customer_id", "status")
    .unionByName(
        df_subscriptions
        .filter(~F.col("plan").isin(["basic", "enterprise"]))
        .select("customer_id", "status")
    )
)
