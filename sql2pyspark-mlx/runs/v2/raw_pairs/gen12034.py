from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .groupBy("country")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum(F.when(F.col("country") == "TH", F.col("rating")).otherwise(0)).alias("th_rating"),
    )
    .filter(F.col("avg_rating") > 27)
    .orderBy("country", F.col("avg_rating").desc())
)
