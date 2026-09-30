from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "North", "Central"]) | (F.col("opened_date") >= "2024-04-15"))
    .groupBy("region")
    .agg(F.count("sqft").alias("sqft_count"))
    .filter(F.col("sqft_count") > 4)
)
