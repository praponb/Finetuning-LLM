from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(F.min("rating").alias("min_rating"))
)
