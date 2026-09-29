from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "SG"]))
    .groupBy("country")
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
    )
    .filter(F.col("avg_rating") < 3.3)
    .orderBy(F.col("avg_rating").desc(), "country")
)
