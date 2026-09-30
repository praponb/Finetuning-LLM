from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.1)
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy("rating_bucket", "city")
    .limit(3)
)
