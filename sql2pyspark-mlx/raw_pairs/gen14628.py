from pyspark.sql import functions as F

result = (
    df_stores
    .select(F.col("store_name").alias("label"), F.lit("store").alias("kind"))
    .unionByName(
        df_customers
        .filter(F.col("is_active"))
        .select(F.col("email").alias("label"), F.lit("customer").alias("kind"))
    )
    .orderBy("kind")
)
