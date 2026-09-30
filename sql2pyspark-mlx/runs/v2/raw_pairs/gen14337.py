from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "store_name", "opened_date", F.trim("city").alias("clean_city"))
    .orderBy("id", "opened_date")
)
