from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "West")
    .select("store_name", "city", "opened_date")
)
