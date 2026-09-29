from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull())
    .select(F.col("monthly_fee").alias("value"))
    .unionByName(
        df_shipments
        .filter(F.col("cost").isNotNull())
        .select(F.col("cost").alias("value"))
    )
)
