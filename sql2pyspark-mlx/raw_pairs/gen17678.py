from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("rating"))).alias("berlin_total_rating"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.avg(F.when(F.col("country") == "SG", F.col("rating"))).alias("sg_avg_rating"),
    )
    .orderBy(F.col("sg_avg_rating").asc_nulls_first())
)
