from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .groupBy("country")
    .agg(
        F.count("rating").alias("rating_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .filter(F.col("rating_count") < 3)
    .orderBy(F.col("avg_rating").desc())
)
