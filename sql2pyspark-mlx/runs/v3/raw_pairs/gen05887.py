from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "West", "South"]))
    .select("id", "opened_date", "region", "store_name")
    .orderBy("store_name")
)
