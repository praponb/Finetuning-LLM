from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 4611)
    .groupBy("region")
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
    .filter(F.col("sqft_range") >= 25)
    .orderBy("sqft_range")
)
