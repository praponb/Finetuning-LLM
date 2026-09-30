from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Bangkok") & (F.lower(F.trim("store_name")) == "mall central"))
    .select("city", F.round((F.col("sqft") * 1.15), 2).alias("sqft_scaled"))
)
