from pyspark.sql import functions as F

result = (
    df_products
    .select("cost", F.lit("product").alias("source"))
    .unionByName(
        df_subscriptions
        .select(F.col("monthly_fee").alias("cost"), F.lit("subscription").alias("source"))
    )
    .orderBy("cost")
)
