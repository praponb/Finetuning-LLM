from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "TH")
    .select("city", F.lit("customer").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("city") != "Chiang Mai")
        .select("city", F.lit("store").alias("source"))
    )
    .distinct()
    .orderBy("city")
)
