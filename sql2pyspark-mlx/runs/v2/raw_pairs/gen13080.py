from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.count("rating").alias("rating_count"))
    .orderBy(F.col("rating_count").desc())
)
