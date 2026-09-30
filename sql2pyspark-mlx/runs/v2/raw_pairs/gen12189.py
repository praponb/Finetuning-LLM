from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.min("rating").alias("min_rating"))
)
