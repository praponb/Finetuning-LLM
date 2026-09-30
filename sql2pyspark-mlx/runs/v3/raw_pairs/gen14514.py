from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNull() & (F.col("opened_date") > "2024-11-27") & F.col("store_name").like("%Siam%"))
    .groupBy("region")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.count("*").alias("store_count"),
    )
    .filter(F.col("store_count") >= 2)
    .drop("store_count")
    .orderBy("region")
)
