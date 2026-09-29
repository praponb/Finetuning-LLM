from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.max("sqft").alias("max_sqft"))
    .filter(F.col("max_sqft") > 7000)
    .orderBy("opened_year", F.col("max_sqft").desc())
    .limit(50)
)
