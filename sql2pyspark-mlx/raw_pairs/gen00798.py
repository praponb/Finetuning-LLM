from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([21, 27, 32, 37]))
    .select(F.lit("subscription").alias("source"), F.col("status").alias("label"))
    .unionByName(
        df_stores
        .select(F.lit("store").alias("source"), F.col("store_name").alias("label"))
    )
    .orderBy(F.col("label").desc())
)
