from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("store_name").isNotNull() & F.col("sqft").between(11524, 15940)) | (F.col("sqft") > 5853))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.sort_array(F.collect_set("region")).alias("region_list"))
    .orderBy(F.col("opened_year").desc())
    .limit(20)
)
