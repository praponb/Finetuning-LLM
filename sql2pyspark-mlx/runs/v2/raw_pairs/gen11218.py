from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "New York") | (F.col("city") == "New York"))
    .cube("country", "city")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)
