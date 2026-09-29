from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.col("store_name").like("Sia%") & ((F.upper(F.trim("store_name")) == "RIVERSIDE") | F.col("region").isin(["East", "Central", "West"])))
    .withColumn("sqft_filled", F.coalesce("sqft", F.lit(0)))
)

region_summary = (
    filtered_stores
    .groupBy("region")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.count("sqft_filled").alias("sqft_filled_count"),
    )
)

result = region_summary.orderBy(F.col("city_values").desc())
