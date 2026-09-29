from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .groupBy("city")
    .agg(
        F.min("rating").alias("min_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
    .orderBy(F.col("unique_countrys").desc())
)
