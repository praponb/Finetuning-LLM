from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
        F.when(F.col("city").isin(["Berlin", "Tokyo"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.sum("rating").alias("total_rating"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .orderBy(F.col("total_rating").desc())
)
