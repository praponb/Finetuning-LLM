from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "TH", 1).otherwise(0))) / F.count("*")), 1).alias("pct_th"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .orderBy(F.col("rating_bucket").desc())
)
