from pyspark.sql import functions as F

result = (
    df_stores
    .select("city", "store_name", "sqft")
    .orderBy("store_name")
)
