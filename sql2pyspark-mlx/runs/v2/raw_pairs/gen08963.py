from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)
