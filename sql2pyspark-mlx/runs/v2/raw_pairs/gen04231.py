from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(11900, 15600))
    .groupBy("city")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)
