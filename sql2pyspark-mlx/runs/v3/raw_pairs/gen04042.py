from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Singapore", "New York", "Chiang Mai"]))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
)
