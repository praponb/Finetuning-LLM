from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .groupBy("city")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("total_rating") >= 15)
    .orderBy("min_rating")
)
