from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") > 9)
    .select("id", "store_name")
    .unionByName(df_stores.filter(F.col("sqft") <= 11481).select("id", "store_name"))
    .distinct()
)
