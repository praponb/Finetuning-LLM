from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.sum("rating").alias("total_rating"),
        F.max("rating").alias("max_rating"),
    )
    .orderBy(F.col("total_rating").desc())
)
