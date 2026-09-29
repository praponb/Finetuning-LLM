from pyspark.sql import functions as F

result = (
    df_customers
    .select("city", F.lit("customer").alias("kind"))
    .unionByName(
        df_stores
        .filter(F.col("region").isNull())
        .select("city", F.lit("store").alias("kind"))
    )
)
