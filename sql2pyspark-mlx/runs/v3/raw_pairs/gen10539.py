from pyspark.sql import functions as F

src = df_stores.filter((F.col("region") != "Central") | (F.col("city") != "Berlin"))

summary = (
    src
    .groupBy("region")
    .agg(
        F.countDistinct("sqft").alias("unique_citys"),
        F.min("sqft").alias("min_sqft"),
    )
)

result = (
    summary
    .filter(F.col("unique_citys") < 6)
    .select("region", "unique_citys", "min_sqft")
    .orderBy(F.col("min_sqft").desc())
)
