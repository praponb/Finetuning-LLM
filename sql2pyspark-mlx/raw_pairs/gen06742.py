from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Singapore")
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.min("sqft").alias("min_sqft"))
    .filter(F.col("min_sqft") > 14600)
    .select("opened_month", "min_sqft")
    .orderBy(F.col("opened_month").desc(), "min_sqft")
    .limit(5)
)
