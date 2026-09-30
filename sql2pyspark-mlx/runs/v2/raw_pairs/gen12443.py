from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") > 6669) & F.col("store_name").like("O%"))
    .groupBy("region", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
)
