from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(~F.col("city").isin(["Chiang Mai", "Tokyo"]) & (F.year("opened_date") >= 2025))
)

summary = (
    filtered_stores
    .groupBy("city")
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
)

result = (
    summary
    .select("city", "avg_sqft")
    .orderBy(F.col("avg_sqft").desc(), F.col("city").desc())
)
