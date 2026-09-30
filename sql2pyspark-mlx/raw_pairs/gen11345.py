from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") != "South") & (F.col("region") == "East"))
    .select("id", "store_name", "city")
    .orderBy("id")
)
