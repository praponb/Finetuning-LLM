from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .filter(F.col("unique_countrys") >= 8)
    .select("country", "city", "unique_countrys", "city_values")
    .orderBy(F.col("country").desc(), F.col("city_values").desc())
)
