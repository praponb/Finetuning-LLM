from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("store_name").like("%Old%") & F.col("city").isin(["London", "Bangkok"])) | F.month("opened_date").isin([6, 8, 9]))
)

stores_stats = (
    base
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft")).otherwise(0)).alias("berlin_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)

result = (
    stores_stats
    .filter(F.col("berlin_sqft") > 37)
    .select("city", "berlin_sqft", "sqft_range")
    .orderBy(F.col("city").desc())
    .limit(5)
)
