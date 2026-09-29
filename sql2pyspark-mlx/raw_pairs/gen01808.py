from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "standard", "basic"]))
    .select("id", "customer_id", "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee") > 190)
        .select("id", "customer_id", "status")
    )
)
