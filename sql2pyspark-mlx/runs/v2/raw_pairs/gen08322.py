from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "UK") & ((F.col("rating") < 4.1) | (F.col("country") != "DE")))
    .groupBy("country")
    .agg(F.min("rating").alias("min_rating"))
)
