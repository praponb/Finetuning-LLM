from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.7)
    .groupBy("city")
    .agg(
        F.count("rating").alias("rating_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .filter(F.col("rating_count") < 1)
    .orderBy("city")
    .limit(5)
)
