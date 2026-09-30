from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "Tokyo")
    .groupBy("city")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.count("rating").alias("rating_count"),
    )
    .orderBy("rating_count")
)
