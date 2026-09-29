from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("jp_rating") < 48)
    .orderBy(F.col("country").desc())
)
