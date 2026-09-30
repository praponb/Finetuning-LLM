from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.sum("rating").alias("total_rating"))
    .orderBy(F.col("country").desc())
)
