from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "JP")
    .groupBy("city", "country")
    .agg(F.min("rating").alias("min_rating"))
    .orderBy(F.col("min_rating").desc(), "country")
)
