from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
    .orderBy("city_values")
)
