from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("region") == "Central") & F.lower("store_name").like("%all%") & (F.col("region") == "Central"))
)

region_summary = (
    base
    .groupBy("region")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"),
    )
)

result = (
    region_summary
    .filter(F.col("london_count") > 73)
    .orderBy("london_count")
)
