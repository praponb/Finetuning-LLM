from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.1)
    .agg(
        F.count("rating").alias("rating_count"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
)
