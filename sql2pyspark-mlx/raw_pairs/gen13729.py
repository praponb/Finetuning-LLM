from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
    )
    .filter(F.col("london_count") < 1)
    .drop("london_count")
    .orderBy("country_values")
)
