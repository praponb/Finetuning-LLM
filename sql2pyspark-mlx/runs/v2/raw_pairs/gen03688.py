from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper(F.trim("store_name")) == "DOWNTOWN EXPRESS")
    .withColumn("clean_store_name", F.trim("store_name"))
    .orderBy(F.col("sqft").desc(), "id")
    .limit(1)
)
