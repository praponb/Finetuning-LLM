from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("sqft") != 12724) & (F.lower(F.trim("store_name")) == "mall central")))
    .groupBy("region")
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
    .orderBy("region")
)
