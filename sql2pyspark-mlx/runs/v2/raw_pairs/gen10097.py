from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.countDistinct("rating").alias("unique_citys"),
        F.count("*").alias("total_count"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("min_rating") >= 1.9)
    .drop("min_rating")
)
