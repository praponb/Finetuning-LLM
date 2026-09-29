from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(6049, 10437) | F.col("region").isin(["Central", "East"]))
    .withColumn("region_lower", F.lower("region"))
    .orderBy("store_name", "id")
    .limit(10)
)
