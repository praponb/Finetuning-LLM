from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-02-05")
    .select("region")
    .unionByName(df_stores.filter(F.col("city") == "Singapore").select("region"))
    .distinct()
)
