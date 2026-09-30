from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .groupBy("country", "city")
    .agg(F.sum("rating").alias("total_rating"))
    .filter(F.col("total_rating") < 17)
)
