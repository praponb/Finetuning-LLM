from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("opened_date") >= "2024-08-01") & F.col("sqft").between(6730, 14149)) | (F.length("store_name") >= 5))
    .groupBy("region")
    .agg(F.sort_array(F.collect_set("region")).alias("region_list"))
)
