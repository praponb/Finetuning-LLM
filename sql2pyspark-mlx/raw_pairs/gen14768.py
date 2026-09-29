from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["standard", "enterprise"]))
    .select("id", "plan")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") == "premium")
        .select("id", "plan")
    )
)
