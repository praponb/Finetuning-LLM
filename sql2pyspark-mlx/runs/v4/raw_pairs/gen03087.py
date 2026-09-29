from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "Tokyo")
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy("city")
)
