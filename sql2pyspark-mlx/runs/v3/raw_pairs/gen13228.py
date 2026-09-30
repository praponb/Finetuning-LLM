from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.count("*").alias("num_suppliers"),
        F.count("rating").alias("rating_count"),
    )
    .filter(F.col("rating_count") < 1)
    .drop("rating_count")
    .orderBy("city_values", "country")
)
