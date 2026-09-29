from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("tokyo_count").desc())

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 14671, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
    )
    .withColumn("tokyo_count_rank", F.rank().over(w))
    .orderBy(F.col("sqft_tier").asc_nulls_first())
)
