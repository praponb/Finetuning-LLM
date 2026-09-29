from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select("end_date", F.lit("subscription").alias("source"))
    .union(
        df_products
        .filter(F.size("tags") >= 2)
        .select("launch_date", F.lit("product").alias("source"))
    )
)
