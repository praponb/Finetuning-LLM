from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(F.min("rating").alias("min_rating"))
    .filter(F.col("min_rating") < 3.3)
)
