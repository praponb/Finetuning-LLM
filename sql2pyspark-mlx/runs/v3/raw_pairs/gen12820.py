from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.count("rating").alias("rating_count"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .filter(F.col("rating_count") < 8)
    .select("city", "rating_count", "rating_range")
)
