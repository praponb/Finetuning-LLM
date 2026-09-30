from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.lower(F.trim("store_name")) == "siam flagship") & (F.col("region").isin(["Central", "East", "South"]) | (F.col("sqft") < 8785)))
    .select("sqft")
    .orderBy("sqft")
)
