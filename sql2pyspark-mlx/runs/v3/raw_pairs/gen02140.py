from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "Tokyo", "Chiang Mai"]) & (F.col("city") == "Tokyo") & F.col("store_name").like("%Cen%"))
    .cube("region", "city")
    .agg(F.countDistinct("sqft").alias("unique_citys"))
    .orderBy(F.col("unique_citys").desc(), F.col("region").desc())
)
