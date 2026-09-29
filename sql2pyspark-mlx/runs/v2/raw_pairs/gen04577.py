from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.5)
    .groupBy("country")
    .agg(F.sum("rating").alias("total_rating"))
    .orderBy("total_rating")
)
