from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"), "region")
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
)
