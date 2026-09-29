from pyspark.sql import functions as F

recent_stores = df_stores.filter(F.col("city") != "Chiang Mai")

region_summary = (
    recent_stores
    .groupBy("region")
    .agg(F.sum("sqft").alias("total_sqft"))
)

result = (
    region_summary
    .select("region", "total_sqft")
    .orderBy("region")
)
