from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper(F.trim("store_name")) == "OLD TOWN")
    .groupBy("city")
    .pivot("region", ["South", "East"])
    .agg(F.max("sqft"))
    .orderBy("city")
)
