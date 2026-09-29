from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["New York", "Chiang Mai", "Berlin"]) & F.col("opened_date").between("2024-10-23", "2025-09-27"))
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.max("sqft").alias("max_sqft"),
    )
    .filter(F.col("max_sqft") > 13200)
    .orderBy("max_sqft")
)
