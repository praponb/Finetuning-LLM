from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 9235)
    .select("id", "city", "store_name")
    .orderBy("city", "id")
    .limit(5)
)
