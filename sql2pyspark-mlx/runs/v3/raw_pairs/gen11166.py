from pyspark.sql import functions as F

result = (
    df_customers
    .select("city", F.lit("customer").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("store_name").like("%Kios%"))
        .select("city", F.lit("store").alias("source"))
    )
    .distinct()
)
