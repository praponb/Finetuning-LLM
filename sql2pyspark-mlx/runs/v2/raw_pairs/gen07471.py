from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("rating").between(2.1, 3.8) & (F.col("country") != "JP") & (F.col("country") == "US"))
)

agg = (
    base
    .groupBy("city")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)

result = agg.orderBy("city_values")
