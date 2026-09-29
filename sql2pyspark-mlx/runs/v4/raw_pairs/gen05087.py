from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("rating_per_berlin"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("rating_per_de"),
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.count("*").alias("cnt"),
        F.max(F.when(F.col("country") == "JP", F.col("rating"))).alias("jp_max_rating"),
    )
    .filter(F.col("rating_per_de") > 0)
    .orderBy(F.col("london_events").desc_nulls_last())
)
