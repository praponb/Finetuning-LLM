from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "London"]) & ~F.col("city").isin(["Singapore", "Bangkok", "London"]))
)

country_summary = (
    recent_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)

result = country_summary.orderBy(F.col("stddev_rating").desc(), F.col("us_count").desc())
