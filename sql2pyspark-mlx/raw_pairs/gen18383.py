from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "South")
    .groupBy("city")
    .agg(
        F.count("*").alias("store_count"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .filter(F.col("store_count") > 4)
    .select("city", "store_count", "first_opened_date")
    .orderBy(F.col("store_count").desc())
)
