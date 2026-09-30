from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
    .orderBy("signup_quarter")
)
