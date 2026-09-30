from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"), "city")
    .agg(F.sort_array(F.collect_set("region")).alias("region_list"))
    .orderBy(F.col("opened_month").desc(), F.col("city").desc())
    .limit(5)
)
