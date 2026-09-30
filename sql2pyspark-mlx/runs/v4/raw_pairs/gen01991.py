from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .orderBy(F.col("country_values").desc(), "country", "city")
    .limit(5)
)
