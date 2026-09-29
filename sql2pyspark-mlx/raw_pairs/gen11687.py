from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .filter(F.col("de_count") >= 7)
    .select("city", "de_count", "pct_chiang_mai")
    .orderBy("de_count", "city")
    .limit(10)
)
