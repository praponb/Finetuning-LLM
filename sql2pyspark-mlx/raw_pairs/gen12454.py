from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
    .orderBy(F.col("region_values").desc())
)
