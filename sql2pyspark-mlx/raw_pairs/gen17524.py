from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.7)
    .agg(F.min("rating").alias("min_rating"))
)
