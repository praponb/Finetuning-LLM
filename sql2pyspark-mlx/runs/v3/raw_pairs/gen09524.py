from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Riv%") & (F.col("sqft") == 12948) & F.col("city").isin(["Berlin", "Tokyo", "Chiang Mai"]))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy(F.col("avg_sqft").desc())
)
