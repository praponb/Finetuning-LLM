from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNull())
    .withColumn("store_name_title", F.initcap(F.trim("store_name")))
    .withColumn("clean_city", F.trim("city"))
)
