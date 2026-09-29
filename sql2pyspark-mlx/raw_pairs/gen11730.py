from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower("store_name").like("%all%") & F.col("sqft").between(8248, 8405))
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
    .filter(F.col("bangkok_count") < 63)
    .orderBy("region")
    .limit(5)
)
