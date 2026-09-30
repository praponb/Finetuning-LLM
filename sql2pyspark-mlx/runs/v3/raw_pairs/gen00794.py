from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date", F.lit("subscription").alias("source"))
    .union(
        df_products
        .filter(F.col("price").between(490, 930))
        .select("launch_date", F.lit("product").alias("source"))
    )
    .distinct()
)
