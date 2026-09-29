from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Tokyo")
    .groupBy(F.year("opened_date").alias("opened_year"), "region")
    .agg(F.sum("sqft").alias("total_sqft"))
    .filter(F.col("total_sqft") < 56000)
    .orderBy("region", F.col("total_sqft").desc())
)
