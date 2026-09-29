from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(90, 230))
    .select("plan")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan").isin(["enterprise", "premium", "standard"]))
        .select("plan")
    )
    .distinct()
)
