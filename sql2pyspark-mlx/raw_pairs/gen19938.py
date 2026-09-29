from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("region")
    .subtract(
        df_stores
        .filter(F.month("opened_date").isin([5, 7, 10]))
        .select("region")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("city")
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
    )
    .filter(F.col("sqft_stddev") > 0)
    .orderBy(F.col("bangkok_sqft").desc())
)
