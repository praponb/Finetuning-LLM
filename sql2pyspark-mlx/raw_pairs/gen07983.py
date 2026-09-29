from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 12760)
    .select("region")
    .unionByName(df_stores.filter(F.col("store_name").like("D%")).select("region"))
    .orderBy("region")
)
