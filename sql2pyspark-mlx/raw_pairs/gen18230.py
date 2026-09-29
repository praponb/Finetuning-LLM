from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy(F.col("city").desc_nulls_first())
)
