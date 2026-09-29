from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("customer_id", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("plan").isin(["standard", "basic", "premium"]))
        .select("customer_id", "id")
    )
)
