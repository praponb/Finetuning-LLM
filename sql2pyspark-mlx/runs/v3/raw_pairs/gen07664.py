from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(F.sum("rating").alias("total_rating"))
    .filter(F.col("total_rating") < 11)
    .select("city", "total_rating")
    .orderBy(F.col("total_rating").desc())
)
