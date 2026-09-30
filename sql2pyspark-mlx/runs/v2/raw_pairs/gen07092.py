from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
    .orderBy(F.col("signup_quarter").desc())
    .limit(20)
)
