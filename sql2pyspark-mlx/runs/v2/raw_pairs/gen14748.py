from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "Bangkok"]))
    .groupBy("city")
    .agg(
        F.count("rating").alias("rating_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)
