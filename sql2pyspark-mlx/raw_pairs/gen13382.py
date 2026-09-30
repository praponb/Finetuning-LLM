from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter((F.col("city") != "Berlin") | (F.col("rating") <= 3.9))
    .withColumn("country_supplier_name", F.concat("country", F.lit("/"), "supplier_name"))
)

summary = (
    filtered_suppliers
    .groupBy("country")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)

result = (
    summary
    .select("country", "city_values")
    .orderBy("city_values")
)
