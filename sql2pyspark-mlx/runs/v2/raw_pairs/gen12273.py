from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)
