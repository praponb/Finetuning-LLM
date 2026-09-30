from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "South", "North"]))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
    .orderBy("opened_year")
)
