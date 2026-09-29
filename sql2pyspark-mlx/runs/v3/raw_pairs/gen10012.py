from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.avg("rating").alias("avg_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .filter(F.col("rating_range") < 60)
    .select("country", "avg_rating", "rating_range")
    .orderBy(F.col("avg_rating").desc(), "country")
    .limit(5)
)
