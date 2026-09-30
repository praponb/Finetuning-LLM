from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Stark", "Umbrella"]))
    .select("cost", F.lit("product").alias("source"))
    .union(
        df_subscriptions
        .select("monthly_fee", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy("cost")
)
