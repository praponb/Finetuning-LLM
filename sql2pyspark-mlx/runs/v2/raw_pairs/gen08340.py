from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower("store_name").like("%ld %"))
    .withColumn("city_title", F.initcap(F.trim("city")))
)
