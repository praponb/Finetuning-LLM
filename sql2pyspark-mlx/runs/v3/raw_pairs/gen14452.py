from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
    .withColumn("store_name_city", F.concat("store_name", F.lit(" "), "city"))
    .orderBy("store_name")
)
