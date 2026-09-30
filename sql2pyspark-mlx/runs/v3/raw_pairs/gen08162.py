from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower(F.trim("store_name")) == "siam flagship")
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)
