from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("city").isin(["Chiang Mai", "Singapore"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .filter(F.col("stddev_rating") < 78)
)
