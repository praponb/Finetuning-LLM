from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull())
    .select("monthly_fee")
    .subtract(
        df_store_sales
        .filter(F.col("revenue").isNotNull())
        .select(F.col("revenue").alias("monthly_fee"))
    )
)
