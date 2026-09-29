from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNotNull())
    .select(F.lit("product").alias("source"), "cost")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select(F.lit("subscription").alias("source"), F.col("monthly_fee").alias("cost"))
    )
    .distinct()
)
