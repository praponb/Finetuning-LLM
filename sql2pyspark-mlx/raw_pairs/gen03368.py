from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull())
    .select(F.lit("subscription").alias("origin"), "monthly_fee")
    .unionByName(
        df_products
        .filter(F.col("cost").isNotNull())
        .select(F.lit("product").alias("origin"), F.col("cost").alias("monthly_fee"))
    )
)
