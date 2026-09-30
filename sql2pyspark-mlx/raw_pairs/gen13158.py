from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("status").alias("label"), F.lit("subscription").alias("record_type"))
    .unionByName(
        df_products
        .filter(F.col("price") <= 620)
        .select(F.col("category").alias("label"), F.lit("product").alias("record_type"))
    )
    .distinct()
)
