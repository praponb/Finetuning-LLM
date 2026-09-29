from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("avg_rating") < 2.1)
    .select("country", "avg_rating", "total_rating")
)
