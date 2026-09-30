from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["West", "South"]) | (F.col("city") == "New York"))
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
    )
    .orderBy("stddev_sqft")
)
