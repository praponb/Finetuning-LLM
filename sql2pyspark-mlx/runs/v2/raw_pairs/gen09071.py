from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
    .orderBy(F.col("city_values").desc(), "country")
    .limit(5)
)
