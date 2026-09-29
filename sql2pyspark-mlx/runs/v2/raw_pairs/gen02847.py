from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(120, 220))
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("status")
    )
    .orderBy("status")
)
