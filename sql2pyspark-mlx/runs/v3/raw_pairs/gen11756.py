from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .agg(F.count("rating").alias("rating_count"))
)
