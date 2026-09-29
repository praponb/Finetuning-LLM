from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "standard", "premium"]))
    .select("id", "status")
)
