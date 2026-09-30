from pyspark.sql import functions as F

result = (
    df_payments
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("status").isin(["PAUSED", "ACTIVE", "EXPIRED"]))
        .select("status")
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
