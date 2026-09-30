from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.year("opened_date") >= 2024) & (F.col("opened_date") < "2024-02-03"))
    .select("city", F.trim("region").alias("clean_region"))
    .orderBy("city")
)
