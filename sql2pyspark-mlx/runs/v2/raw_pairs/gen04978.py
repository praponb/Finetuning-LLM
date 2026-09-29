from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("rating").alias("rating_count"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
    .orderBy("avg_rating")
)
