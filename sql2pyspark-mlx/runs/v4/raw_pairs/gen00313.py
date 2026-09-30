from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
        F.avg("rating").alias("avg_rating"),
    )
    .orderBy(F.col("avg_rating").desc())
)
