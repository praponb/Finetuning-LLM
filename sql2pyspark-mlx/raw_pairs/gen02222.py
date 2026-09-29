from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNotNull())
    .select(F.lit("product").alias("origin"), "cost")
    .union(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select(F.lit("subscription").alias("origin"), "monthly_fee")
    )
    .orderBy("cost")
)
