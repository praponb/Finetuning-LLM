from pyspark.sql import functions as F

valid_customers = df_customers.filter(~((F.col("age") >= 41) & (F.col("country") != "US")))

agg = (
    valid_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = agg.orderBy(F.col("signup_quarter").desc())
