from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(F.max("rating").alias("max_rating"))
    .filter(F.col("max_rating") >= 2.3)
    .orderBy("city")
)
