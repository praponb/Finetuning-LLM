from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Berlin", "Bangkok", "New York"]))
    .groupBy(
        "country",
        "city",
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
        F.sum("rating").alias("total_rating"),
    )
)
