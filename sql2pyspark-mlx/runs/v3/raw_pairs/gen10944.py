from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("rating").alias("rating_count"),
    )
    .filter(F.col("total_rating") >= 12)
    .select("city", "total_rating", "rating_count")
    .orderBy("total_rating", F.col("rating_count").desc())
)
