from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy(F.col("avg_rating").desc())
)
