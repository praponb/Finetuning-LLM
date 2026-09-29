from pyspark.sql import functions as F

base = df_stores.filter(F.col("opened_date") < "2024-01-01")

stores_stats = (
    base
    .groupBy("city")
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.sum("sqft").alias("total_sqft"),
    )
)

result = (
    stores_stats
    .filter(F.col("unique_regions") < 9)
    .select("city", "unique_regions", "total_sqft")
    .orderBy(F.col("total_sqft").desc(), "unique_regions", "city")
    .limit(50)
)
