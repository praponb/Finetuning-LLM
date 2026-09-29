from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "opened_date", F.lower("store_name").alias("store_name_lower"))
)
