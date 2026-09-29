from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("region") == "East"))
    .withColumn("sqft_scaled", F.round((F.col("sqft") * 1.1), 2))
    .withColumn("region_filled", F.coalesce("region", F.lit("UNKNOWN")))
)

result = (
    valid
    .groupBy(
        F.when(F.col("sqft_scaled") >= 65, "LARGE").otherwise("SMALL").alias("sqft_scaled_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "East", 1)), F.lit(0)), 2).alias("sqft_per_east"),
    )
)
