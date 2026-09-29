from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .groupBy("city")
    .agg(
        F.min("rating").alias("min_rating"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("min_rating") > 3.8)
)
