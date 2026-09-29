from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("city")
    .intersect(
        df_stores
        .filter(F.col("region") == "East")
        .select("city")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("region")
    .agg(
        F.countDistinct("region").alias("distinct_regions"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "Central", 1)), F.lit(0)), 2).alias("sqft_per_central"),
        F.max(F.when(F.col("region") == "West", F.col("sqft"))).alias("west_max_sqft"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
