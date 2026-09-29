from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"),
        F.sum(F.when(F.col("city") == "New York", F.col("rating")).otherwise(0)).alias("new_york_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)
