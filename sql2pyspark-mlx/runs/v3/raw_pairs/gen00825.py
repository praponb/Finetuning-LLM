from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Bangkok", "Chiang Mai", "New York"]))
    .groupBy(F.year("opened_date").alias("opened_year"), "city")
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
    .filter(F.col("min_sqft") >= 11900)
    .select("opened_year", "city", "min_sqft", "unique_regions")
)
