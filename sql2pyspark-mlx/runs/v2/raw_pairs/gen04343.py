from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .withColumn("store_name_normalized", F.upper(F.trim("store_name")))
    .orderBy(F.col("store_name").desc(), "id")
    .limit(50)
)
