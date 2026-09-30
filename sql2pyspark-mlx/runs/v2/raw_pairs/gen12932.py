from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") == 2023)
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .orderBy("unique_citys")
)
