from pyspark.sql import functions as F

result = (
    df_payments
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee").between(100, 190))
        .select("status")
    )
    .distinct()
    .orderBy("status")
)
