from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("avg_rating") > 4.1)
    .select("country", "rating_range", "avg_rating")
)
