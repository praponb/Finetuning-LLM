from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") < 11893)
    .select("store_name")
    .unionByName(df_stores.filter(F.col("region") == "Central").select("store_name"))
    .distinct()
)
