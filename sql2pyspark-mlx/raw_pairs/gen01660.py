from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", "region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
)
