from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.when(F.col("sqft") >= 17237, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("sqft_per_london"),
    )
)
