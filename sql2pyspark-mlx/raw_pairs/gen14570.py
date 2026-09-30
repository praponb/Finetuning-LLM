from pyspark.sql import functions as F

result = (
    df_customers
    .select("city", F.lit("customer").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("region").isin(["Central", "West", "East"]))
        .select("city", F.lit("store").alias("source"))
    )
    .orderBy(F.col("city").desc())
)
