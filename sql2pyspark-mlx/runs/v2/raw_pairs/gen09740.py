from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "city")
    .orderBy("city", "id")
    .limit(10)
)
