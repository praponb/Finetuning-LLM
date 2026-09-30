from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") >= 13)
    .withColumn("city_store_name", F.concat("city", F.lit(" "), "store_name"))
    .withColumn("store_name_city", F.concat("store_name", F.lit("/"), "city"))
)
