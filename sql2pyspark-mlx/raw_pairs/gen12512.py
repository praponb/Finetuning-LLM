from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(F.min("rating").alias("min_rating"))
    .filter(F.col("min_rating") >= 1.9)
    .select("city", "min_rating")
)
