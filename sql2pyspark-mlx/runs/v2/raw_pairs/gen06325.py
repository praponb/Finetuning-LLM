from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 2.2)
    .groupBy("city")
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"),
    )
)
