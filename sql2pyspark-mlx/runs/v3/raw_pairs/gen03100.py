from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.max("rating").alias("max_rating"))
)
