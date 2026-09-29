from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city", "country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.max("rating").alias("max_rating"),
    )
)
