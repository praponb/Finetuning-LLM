from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .filter(F.col("pct_chiang_mai") < 25)
    .orderBy(F.col("rating_range").desc(), F.col("pct_chiang_mai").desc(), "country")
    .limit(10)
)
