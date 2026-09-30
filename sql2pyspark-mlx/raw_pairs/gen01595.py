from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status", "customer_id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("monthly_fee") > 190)
        .select("status", "customer_id")
    )
)
