from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["New York", "Berlin"])
    .agg(F.sum("sqft").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "region",
        F.col("New York_total").alias("new_york_total"),
        F.col("New York_n").alias("new_york_n"),
        F.col("Berlin_total").alias("berlin_total"),
        F.col("Berlin_n").alias("berlin_n"),
    )
)
