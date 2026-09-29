from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(200, 230))
    .select("id", "customer_id")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee") >= 90)
        .select("id", "customer_id")
    )
    .distinct()
    .orderBy("customer_id")
)
