from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"), "city")
    .agg(
        F.count("*").alias("num_stores"),
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
    )
    .filter(F.col("tokyo_count") >= 48)
    .drop("tokyo_count")
    .orderBy(F.col("num_stores").desc())
)
