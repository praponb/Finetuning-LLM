from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(4534, 5118))
    .withColumn("city_length", F.length("city"))
    .withColumn("clean_city", F.trim("city"))
    .orderBy(F.col("opened_date").desc(), "id")
    .limit(3)
)
