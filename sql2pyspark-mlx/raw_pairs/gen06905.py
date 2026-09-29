from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .groupBy("country")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .filter(F.col("avg_rating") < 22)
)
