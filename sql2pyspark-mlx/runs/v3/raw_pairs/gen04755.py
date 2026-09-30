from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("city_length", F.length("city"))
    .orderBy(F.col("region").desc())
)
