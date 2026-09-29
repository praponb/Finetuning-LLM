from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .groupBy("city")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)
