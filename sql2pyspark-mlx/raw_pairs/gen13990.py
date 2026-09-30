from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Singapore", "Chiang Mai"]))
    .groupBy("country", "city")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating")).otherwise(0)).alias("chiang_mai_rating"),
    )
    .filter(F.col("chiang_mai_rating") >= 53)
    .drop("chiang_mai_rating")
)
