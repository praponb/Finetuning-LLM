from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 690)
    .select("launch_date", F.lit("product").alias("source"))
    .union(
        df_subscriptions
        .filter(F.month("start_date").isin([6, 9, 12]))
        .select("end_date", F.lit("subscription").alias("source"))
    )
)
