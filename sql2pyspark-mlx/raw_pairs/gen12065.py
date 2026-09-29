from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["East", "Central"]) & (F.col("city").isin(["New York", "Chiang Mai"]) | (F.col("city") == "Chiang Mai")))
    .cube(F.year("opened_date").alias("opened_year"), "city")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count("*").alias("store_count"),
    )
    .orderBy("store_count")
)
