from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Singapore", "Bangkok", "Tokyo"]))
    .groupBy("region")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
    )
)
