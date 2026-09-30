from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-06-01", "2025-09-01") & ~F.col("region").isin(["West", "South"]))
    .groupBy("city")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
    .orderBy(F.col("max_sqft").desc())
)
