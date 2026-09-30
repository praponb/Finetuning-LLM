from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNotNull())
    .select(F.lit("product").alias("kind"), F.col("cost").alias("value"))
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select(F.lit("subscription").alias("kind"), F.col("monthly_fee").alias("value"))
    )
    .orderBy(F.col("value").desc())
)
