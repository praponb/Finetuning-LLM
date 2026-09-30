from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date", F.concat_ws("/", "region", "city").alias("region_city_key"))
)
