from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select("city", F.lit("customer").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("city") == "Bangkok")
        .select("city", F.lit("store").alias("source"))
    )
    .distinct()
)
