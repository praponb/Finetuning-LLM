from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([2, 4, 9]))
    .select("cost", F.lit("product").alias("source"))
    .union(
        df_subscriptions
        .select("monthly_fee", F.lit("subscription").alias("source"))
    )
    .distinct()
)
