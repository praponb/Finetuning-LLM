from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("city")
    .subtract(
        df_stores
        .filter(F.col("opened_date") <= "2024-09-01")
        .select("city")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(F.when(F.col("sqft") >= 16744, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
    )
    .filter(F.col("bangkok_sqft") > 0)
    .orderBy("bangkok_rows")
)
