from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
)
