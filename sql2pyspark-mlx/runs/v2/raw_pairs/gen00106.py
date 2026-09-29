from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city", "country")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
    .filter(F.col("bangkok_count") > 72)
)
