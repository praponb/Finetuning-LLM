from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") > 8527) & (F.col("sqft") == 10720))
    .select("store_name", F.upper("region").alias("region_upper"))
    .orderBy("region_upper")
)
