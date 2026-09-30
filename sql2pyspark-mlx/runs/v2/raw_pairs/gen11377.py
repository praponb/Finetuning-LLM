from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "city", "region")
    .orderBy("id", "city")
)
