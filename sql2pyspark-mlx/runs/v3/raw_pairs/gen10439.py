from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum("rating").alias("total_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .filter(F.col("rating_range") > 46)
    .orderBy(F.col("country").desc())
)
